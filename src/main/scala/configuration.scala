package bayenv

import ohnosequences.statika._
import ohnosequences.statika.ami.AMI149f7863
import ohnosequences.nispero._
import ohnosequences.nispero.bundles._
import ohnosequences.nispero.bundles.console.{Console, FarmStateLogger}
import ohnosequences.awstools.ec2.{EC2, InstanceSpecs}
import ohnosequences.awstools.ec2.InstanceType._
import ohnosequences.awstools.s3.{S3, ObjectAddress}
import ohnosequences.awstools.autoscaling._
import ohnosequences.nispero.bundles.{NisperoDistribution, Worker, Configuration}
import ohnosequences.nispero.manager.ManagerAutoScalingGroups
import ohnosequences.nispero.worker.WorkersAutoScalingGroup
import java.io.File
import ohnosequences.typesets._
import shapeless._
import scala.io.Source

case class SNPsProvider(bucket: String, project: String, snpsFile: String, covMatrixFile: String, envVarsFile: String) extends TasksProvider {

  def objAddress(suffix: String): ObjectAddress = ObjectAddress(bucket, project+"/"+suffix)

  def tasks(s3: S3): Stream[Task] = {
    val snpsObj = objAddress(snpsFile)
    val snpsStream = s3.s3.getObject(snpsObj.bucket, snpsObj.key).getObjectContent

    Source.fromInputStream(snpsStream).getLines.sliding(2, 2).zipWithIndex.sliding(20, 20).map { chunk =>

      Task("chunk["+chunk.head._2+"-"+chunk.last._2+"]",
        chunk.map{ case (snp, i) => "snp"+i+".inp" -> snp.mkString("\n") }.toMap,
        chunk.map{ case (snp, i) => "snp"+i+".out" -> objAddress("output2/snp"+i+".out") }.toMap
      )

    }.toStream
  }
}

object consts {
  val bucket = "bayenv-testing"

  val tasksProvider = SNPsProvider(bucket,
    project       = "bayenv2_autos_nosingle",
    snpsFile      = "input/bayenv2_autos_nosingle.inp",
    covMatrixFile = "input/covMatrix_median_autos_Nov27.txt",
    envVarsFile   = "input/env_variables_standardized_v1.txt"
  )
  // SNPsProvider("data100Bayenv", 
  //   snpsFile      = "data100Bayenv.inp",
  //   covMatrixFile = "covMatrix",
  //   envVarsFile   = "envVars"
  // ),
  // SNPsProvider("bayenv2_chrX_nosingle",
  //   snpsFile      = "bayenv2_chrX_nosingle.inp",
  //   covMatrixFile = "covMatrix_median_X_Nov27.txt",
  //   envVarsFile   = "env_variables_standardized_v1.txt"
  // ),
}

case object configuration extends Configuration {

  type Metadata = generated.metadata.bayenv
  val  metadata = new generated.metadata.bayenv()  

  val version = generateId(metadata)
  
  type AMI = AMI149f7863.type
  val  ami = AMI149f7863

  val specs = InstanceSpecs(
    instanceType = InstanceType("t1.micro"),
    amiId = ami.id,
    securityGroups = List("nispero"),
    keyName = "munich",
    instanceProfile = Some("compota")
  )

  val config = Config(

    email = "aalekhin@ohnosequences.com",

    managerConfig = ManagerConfig(
      groups = ManagerAutoScalingGroups(
        instanceSpecs = specs.copy(instanceType = InstanceType("m3.medium")),
        version = version,
        purchaseModel = SpotAuto
      ),
      password = "15ea88a619"
    ),

    tasksProvider = consts.tasksProvider,

    //sets working directory to ephemeral storage
    workersDir = "/media/ephemeral0",

    // maximum time for processing task
    taskProcessTimeout = 60 * 60 * 1000,

    resources = Resources(id = version)(
      workersGroup = WorkersAutoScalingGroup(
        desiredCapacity = 1,
        version = version,
        instanceSpecs = specs.copy(deviceMapping = Map("/dev/xvdb" -> "ephemeral0"))
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
  val provider = consts.tasksProvider

  val configureScript = s"""
    |echo "configuring"
    |curl http://www.eve.ucdavis.edu/gmcoop/bayenv2_64bit.tar.gz > bayenv2.tar.gz
    |tar xvzf bayenv2.tar.gz
    |chmod a+x bayenv2
    |aws s3 cp --region eu-west-1 s3://${provider.bucket}/${provider.project}/${provider.covMatrixFile} covMatrix
    |aws s3 cp --region eu-west-1 s3://${provider.bucket}/${provider.project}/${provider.envVarsFile} envVars
    |""".stripMargin

  val instructionsScript = """
    |for snp in $(ls input/snp*.inp); do
    |    out=${snp#input/}
    |    out=${out%.inp}.out
    |    for i in {1..10}; do
    |        /root/applicator/bayenv2 -i $snp -m /root/applicator/covMatrix -e /root/applicator/envVars -p 4 -k 10000 -n 6 -t -o $out
    |    done
    |    cat ${out}.bf | cut -f 2- > output/$out
    |done
    |exitcode=$?
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

