import org.gradle.api.DefaultTask
import org.gradle.api.file.ConfigurableFileCollection
import org.gradle.api.file.DirectoryProperty
import org.gradle.api.file.RegularFileProperty
import org.gradle.api.tasks.*
import org.gradle.workers.WorkAction
import org.gradle.workers.WorkParameters
import org.gradle.workers.WorkerExecutor
import org.jetbrains.kotlin.cli.jvm.compiler.EnvironmentConfigFiles
import org.jetbrains.kotlin.cli.jvm.compiler.KotlinCoreEnvironment
import org.jetbrains.kotlin.com.intellij.openapi.util.Disposer
import org.jetbrains.kotlin.config.CommonConfigurationKeys
import org.jetbrains.kotlin.config.CompilerConfiguration
import org.jetbrains.kotlin.cli.common.messages.MessageCollector
import org.jetbrains.kotlin.psi.*
import org.jetbrains.kotlin.psi.psiUtil.getSuperNames
import javax.inject.Inject

abstract class ExtensionGeneratorTask : DefaultTask() {

    @get:InputDirectory
    abstract val inputDir: DirectoryProperty

    @get:OutputFile
    abstract val outputFile: RegularFileProperty

    @get:InputFiles
    @get:Classpath
    abstract val compilerClasspath: ConfigurableFileCollection

    @get:Inject
    abstract val workerExecutor: WorkerExecutor

    @TaskAction
    fun generate() {
        val workQueue = workerExecutor.classLoaderIsolation {
            classpath.from(compilerClasspath)
            classpath.from(this@ExtensionGeneratorTask::class.java.protectionDomain.codeSource.location)
        }

        workQueue.submit(GenerateAction::class.java) {
            inputDir.set(this@ExtensionGeneratorTask.inputDir)
            outputFile.set(this@ExtensionGeneratorTask.outputFile)
        }
    }
}

// Parameters passed to the worker
interface GenerateParameters : WorkParameters {
    val inputDir: DirectoryProperty
    val outputFile: RegularFileProperty
}

abstract class GenerateAction : WorkAction<GenerateParameters> {

    // Helper classes for parsing logic
    data class InterfaceInfo(val name: String, val properties: List<PropInfo>)
    data class PropInfo(val name: String, val type: String, val doc: String)
    data class ResultClassInfo(val className: String, val typeParams: List<String>, val componentNames: List<String>)

    override fun execute() {
        val disposable = Disposer.newDisposable()
        try {
            val configuration = CompilerConfiguration()
            configuration.put(CommonConfigurationKeys.MESSAGE_COLLECTOR_KEY, MessageCollector.NONE)

            val env = KotlinCoreEnvironment.createForProduction(
                disposable, configuration, EnvironmentConfigFiles.JVM_CONFIG_FILES
            )

            // Parsing Logic
            val inputDir = parameters.inputDir.get().asFile
            val ktFiles = inputDir.walkTopDown().filter { it.extension == "kt" }.map { file ->
                KtPsiFactory(env.project).createFile(file.name, file.readText())
            }.toList()

            // 1. Build Inheritance Map (Class Name -> List of Super Names)
            // This allows us to traverse the hierarchy even if the parent is in a different file
            val inheritanceMap = mutableMapOf<String, List<String>>()
            ktFiles.forEach { file ->
                file.declarations.filterIsInstance<KtClass>().forEach { ktClass ->
                    ktClass.name?.let { name ->
                        inheritanceMap[name] = ktClass.getSuperNames()
                    }
                }
            }

            // 2. Recursive Check Function
            // Returns true if 'name' is "Result" or if any of its ancestors are "Result"
            fun isResultDescendant(name: String, visited: Set<String> = emptySet()): Boolean {
                if (name == "Result") return true
                if (name in visited) return false // Prevent infinite loops in case of cyclic inheritance

                // If the class isn't in our map, we assume it's not relevant or external
                // (unless "Result" is external, but we handled the base case above)
                val parents = inheritanceMap[name] ?: return false

                return parents.any { parentName ->
                    isResultDescendant(parentName, visited + name)
                }
            }

            val interfaces = mutableListOf<InterfaceInfo>()
            val results = mutableListOf<ResultClassInfo>()

            ktFiles.forEach { ktFile ->
                ktFile.declarations.filterIsInstance<KtClass>().forEach { ktClass ->
                    val name = ktClass.name ?: return@forEach

                    if (ktClass.isInterface()) {
                        // Check hierarchy recursively
                        if (!isResultDescendant(name)) return@forEach

                        val props = ktClass.declarations.filterIsInstance<KtProperty>().map { prop ->
                            PropInfo(
                                name = prop.name!!,
                                type = prop.typeReference?.text ?: "Any",
                                doc = prop.docComment?.text ?: ""
                            )
                        }.filterNot { it.name == "name"}

                        if (props.isNotEmpty()) interfaces.add(InterfaceInfo(name, props))

                    } else if (name.matches(Regex("Result\\d"))) {
                        val typeParams = ktClass.typeParameters.map { it.name!! }
                        val primaryConstructor = ktClass.primaryConstructor
                        if (typeParams.isNotEmpty() && primaryConstructor != null) {
                            val componentNames = primaryConstructor.valueParameters.mapNotNull { it.name }
                            if (typeParams.size == componentNames.size) {
                                results.add(ResultClassInfo(name, typeParams, componentNames))
                            }
                        }
                    }
                }
            }

            // Generation Logic
            val sb = StringBuilder()
            sb.append("package com.eignex.kumulant.core\n\n")
            sb.append("// GENERATED CODE - DO NOT MODIFY\n")
            sb.append("import kotlin.jvm.JvmName\n\n")

            results.forEach { res ->
                // Track generated properties per component (index) to prevent duplicates
                val generatedPropsPerComponent = Array(res.typeParams.size) { mutableSetOf<String>() }

                interfaces.forEach { iface ->
                    iface.properties.forEach { prop ->
                        res.typeParams.forEachIndexed { index, genericParamName ->
                            // Check if this property name was already generated for this specific component
                            if (prop.name in generatedPropsPerComponent[index]) return@forEachIndexed

                            val componentName = res.componentNames[index]
                            val typeArgs = res.typeParams.mapIndexed { i, _ ->
                                if (i == index) genericParamName else "*"
                            }.joinToString(", ")

                            val jvmName = "get${prop.name.replaceFirstChar { it.uppercase() }}${index + 1}"

                            if (prop.doc.isNotEmpty()) sb.append("${prop.doc}\n")
                            sb.append("@get:JvmName(\"$jvmName\")\n")
                            sb.append("val <$genericParamName : ${iface.name}> ${res.className}<$typeArgs>.${prop.name}: ${prop.type} ")
                            sb.append("get() = $componentName.${prop.name}\n\n")

                            generatedPropsPerComponent[index].add(prop.name)
                        }
                    }
                }
            }

            parameters.outputFile.get().asFile.apply {
                parentFile.mkdirs()
                writeText(sb.toString())
            }

        } finally {
            Disposer.dispose(disposable)
        }
    }
}
