package bayenv

import ohnosequences.statika._
import ohnosequences.statika.ami.AMI149f7863
import ohnosequences.nispero._
import ohnosequences.nispero.bundles._
import ohnosequences.nispero.bundles.console.{Console, FarmStateLogger}
import ohnosequences.awstools.ec2.{EC2, InstanceType, InstanceSpecs}
import ohnosequences.awstools.s3.{S3, ObjectAddress}
import ohnosequences.awstools.autoscaling._
import ohnosequences.nispero.bundles.{NisperoDistribution, Worker, Configuration}
import ohnosequences.nispero.manager.ManagerAutoScalingGroups
import ohnosequences.nispero.worker.WorkersAutoScalingGroup
import java.io.File
import ohnosequences.typesets._
import shapeless._
import scala.io.Source

case class SNPsProvider(providerName: String, snpsFile: String, covMatrixFile: String, envVarsFile: String) extends TasksProvider {

  val bucket = "bayenv-testing"
  val inputKey  = "snp.inp"
  val outputKey = "snp.out"
  val covMatrix = "covMatrix"
  val envVars = "envVars"

  def objAddress(name: String): ObjectAddress = ObjectAddress(bucket,  providerName+"/"+name)

  def tasks(s3: S3): List[Task] = {
    // s3.createBucket(bucket)
    // s3.putObject(objAddress(covMatrix), covMatrixFile)
    // s3.putObject(objAddress(envVars), envVarsFile)

    val snpsObj = objAddress("input/"+snpsFile)
    val objectStream = s3.s3.getObject(snpsObj.bucket, snpsObj.key).getObjectContent
    Source.fromInputStream(objectStream).getLines.sliding(2, 2).zipWithIndex.map { case (snp, i) =>

      val inputObject  = objAddress("input/snps/"+inputKey+i.toString)
      s3.putWholeObject(inputObject, snp.mkString("\n"))

      Task("snp"+i.toString,
        Map(
          inputKey -> inputObject,
          covMatrix -> objAddress("input/"+covMatrixFile),
          envVars -> objAddress("input/"+envVarsFile)
        ),
        Map(outputKey -> objAddress("output/"+outputKey+i.toString))
      )

    }.toList
  }
}

case object configuration extends Configuration {

  type Metadata = generated.metadata.bayenv
  val  metadata = new generated.metadata.bayenv()  

  val version = generateId(metadata)
  
  type AMI = AMI149f7863.type
  val  ami = AMI149f7863

  val specs = InstanceSpecs(
    instanceType = InstanceType.C1Medium,
    amiId = ami.id,
    securityGroups = List("nispero"),
    keyName = "munich",
    instanceProfile = Some("compota")
  )

  val config = Config(

    email = "aalekhin@ohnosequences.com",

    managerConfig = ManagerConfig(
      groups = ManagerAutoScalingGroups(
        instanceSpecs = specs.copy(instanceType = InstanceType.C1Medium),
        version = version,
        purchaseModel = SpotAuto
      ),
      password = "15ea88a619"
    ),

    tasksProvider = 
      SNPsProvider("data100Bayenv", 
        snpsFile      = "data100Bayenv.inp",
        covMatrixFile = "covMatrix",
        envVarsFile   = "envVars"
      ),
      //  ~ SNPsProvider("bayenv2_chrX_nosingle",
      //   snpsFile      = "bayenv2_chrX_nosingle.inp",
      //   covMatrixFile = "covMatrix_median_X_Nov27.txt",
      //   envVarsFile   = "env_variables_standardized_v1.txt"
      // ),

    //sets working directory to ephemeral storage
    workersDir = "/media/ephemeral0",

    //maximum time for processing task
    taskProcessTimeout = 60 * 60 * 1000,

    resources = Resources(id = version)(
      workersGroup = WorkersAutoScalingGroup(
        desiredCapacity = 1,
        version = version,
        instanceSpecs = specs.copy(
          instanceType = InstanceType.InstanceType("t1.micro"),
          deviceMapping = Map("/dev/xvdb" -> "ephemeral0")
        )
      )
    ),

    terminationConditions = TerminationConditions(
      terminateAfterInitialTasks = true
    ),

    jarAddress = getAddress(metadata.artifactUrl)
  )
}

// TODO: write a bundle for this
case object instructions extends ScriptExecutor() {
  val configureScript = """
    |echo "configuring"
    |curl http://www.eve.ucdavis.edu/gmcoop/bayenv2_64bit.tar.gz > bayenv2.tar.gz
    |tar xvzf bayenv2.tar.gz
    |chmod a+x bayenv2
    |mv bayenv2 /media/ephemeral0/
    |""".stripMargin

  val instructionsScript = """
    |./bayenv2 -i input/snp.inp -m input/covMatrix -e input/envVars -p 4 -k 10000 -n 6 -t -o snp.out
    |exitcode=$?
    |mv snp.out.bf output/snp.out
    |(( $exitcode == 0 )) && echo "success" > message || echo "failure" > message
    |exit $exitcode
    |""".stripMargin
}

case object aws extends AWS(configuration)

case object resources extends bundles.Resources(configuration, aws)

case object logUploader extends LogUploader(resources, aws)

case object worker extends Worker(instructions, resources, logUploader, aws)

case object controlQueueHandler extends ControlQueueHandler(resources, aws)

case object terminationDaemon extends TerminationDaemon(resources, aws)

case object manager extends Manager(controlQueueHandler, terminationDaemon, resources, logUploader, aws, worker)

case object farmStateLogger extends FarmStateLogger(resources, aws)

case object console extends Console(resources, logUploader, farmStateLogger, aws)

case object nisperoDistribution extends NisperoDistribution(manager, console)

object nisperoCLI extends NisperoRunner(nisperoDistribution) {
  def compilerChecks() {
    manager.installWithDeps(worker)  
    nisperoDistribution.installWithDeps(console) 
    nisperoDistribution.installWithDeps(manager)
  }
}

