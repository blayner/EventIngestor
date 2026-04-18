namespace EventIngestor.Core.Serialization;

public enum ParseStatus
{
    Ok = 0,
    InvalidJson,
    InvalidField,
    MissingSource,
    MissingType,
    PayloadTooLarge,
}
