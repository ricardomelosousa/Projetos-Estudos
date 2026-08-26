FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS base

WORKDIR /app

EXPOSE 8080

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build

WORKDIR /src

COPY ["src/Services/Orders/Trading.Orders.Api/Trading.Orders.Api.csproj", "src/Services/Orders/Trading.Orders.Api/"]

RUN dotnet restore \
    "src/Services/Orders/Trading.Orders.Api/Trading.Orders.Api.csproj"

COPY . .

WORKDIR "/src/src/Services/Orders/Trading.Orders.Api"

RUN dotnet build \
    "Trading.Orders.Api.csproj" \
    -c Release \
    -o /app/build

FROM build AS publish

RUN dotnet publish \
    "Trading.Orders.Api.csproj" \
    -c Release \
    -o /app/publish \
    /p:UseAppHost=false

FROM base AS final

WORKDIR /app

COPY --from=publish /app/publish .

ENTRYPOINT ["dotnet", "Trading.Orders.Api.dll"]