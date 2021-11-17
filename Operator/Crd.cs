using ContainerSolutions.OperatorSDK;

namespace SqueedBrew2021Operator;

public class Crd : BaseCRD
{
    public Crd() :
        base("samples.k8s-dotnet-controller-sdk", "v1", "mssqldbs", "mssqldb")
    {
    }

    public CrdSpec Spec { get; set; }

    public override bool Equals(object? obj)
    {
        return obj != null && ToString().Equals(obj.ToString());
    }

    public override int GetHashCode()
    {
        return ToString().GetHashCode();
    }

    public override string ToString()
    {
        return Spec.ToString();
    }
}