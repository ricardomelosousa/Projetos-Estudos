FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS base

WORKDIR /app

EXPOSE 8080

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build

WORKDIR /src

COPY ["src/Services/Gateway/Trading.Gateway/Trading.Gateway.csproj", "src/Services/Gateway/Trading.Gateway/"]

RUN dotnet restore \
    "src/Services/Gateway/Trading.Gateway/Trading.Gateway.csproj"

COPY . .

WORKDIR "/src/src/Services/Gateway/Trading.Gateway"

RUN dotnet build \
    "Trading.Gateway.csproj" \
    -c Release \
    -o /app/build

FROM build AS publish

RUN dotnet publish \
    "Trading.Gateway.csproj" \
    -c Release \
    -o /app/publish \
    /p:UseAppHost=false

FROM base AS final

WORKDIR /app

COPY --from=publish /app/publish .

ENTRYPOINT ["dotnet", "Trading.Gateway.dll"]