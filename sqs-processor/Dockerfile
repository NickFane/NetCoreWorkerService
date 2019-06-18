#Depending on the operating system of the host machines(s) that will build or run the containers, the image specified in the FROM statement may need to be changed.
#For more information, please see https://aka.ms/containercompat

FROM mcr.microsoft.com/dotnet/core/runtime:3.0-nanoserver-1803 AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/core/sdk:3.0-nanoserver-1803 AS build
WORKDIR /src
COPY ["sqs-processor/sqs-processor.csproj", "sqs-processor/"]
RUN dotnet restore "sqs-processor/sqs-processor.csproj"
COPY . .
WORKDIR "/src/sqs-processor"
RUN dotnet build "sqs-processor.csproj" -c Release -o /app

FROM build AS publish
RUN dotnet publish "sqs-processor.csproj" -c Release -o /app

FROM base AS final
WORKDIR /app
COPY --from=publish /app .
ENTRYPOINT ["dotnet", "sqs-processor.dll"]