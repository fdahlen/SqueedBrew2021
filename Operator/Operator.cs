using ContainerSolutions.OperatorSDK;
using SqueedBrew2021Operator;


var k8sNamespace = "default";
if (args.Length > 1)
{
    k8sNamespace = args[0];
}

var handler = new OperationHandler();
var sdkController = new Controller<Crd>(new Crd(), handler, k8sNamespace);

await sdkController.SatrtAsync();