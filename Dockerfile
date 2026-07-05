# syntax=docker/dockerfile:1

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

# Restore in its own layer: only invalidated when project files or package versions change.
COPY global.json Directory.Build.props Directory.Packages.props ./
COPY src/EventIngestor.Core/EventIngestor.Core.csproj src/EventIngestor.Core/
COPY src/EventIngestor.Pipeline/EventIngestor.Pipeline.csproj src/EventIngestor.Pipeline/
COPY src/EventIngestor.Service/EventIngestor.Service.csproj src/EventIngestor.Service/
RUN dotnet restore src/EventIngestor.Service/EventIngestor.Service.csproj

COPY src/ src/
RUN dotnet publish src/EventIngestor.Service/EventIngestor.Service.csproj \
      --configuration Release \
      --no-restore \
      --output /app \
      -p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS runtime
WORKDIR /app

# Directory for the file sink; a volume is usually mounted here.
RUN mkdir -p /data/events && chown -R $APP_UID /data

ENV ASPNETCORE_ENVIRONMENT=Production \
    DOTNET_gcServer=1 \
    Ingestor__Sinks__File__OutputDirectory=/data/events

COPY --from=build /app .

# Non-root user shipped with the .NET images.
USER $APP_UID

# 8080: REST, health, metrics (HTTP/1.1) - 8081: gRPC (HTTP/2 cleartext)
EXPOSE 8080 8081

ENTRYPOINT ["dotnet", "EventIngestor.Service.dll"]
