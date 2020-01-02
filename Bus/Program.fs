open Microsoft.Extensions.Configuration
open Microsoft.Extensions.DependencyInjection
open Rebus.Bus
open Rebus.Config
open Serilog
open System
open Topper
open Rebus.ServiceProvider
open Rebus.Retry.Simple
open Rebus.Auditing.Messages
open Rebus.Transport.InMem
open Rebus.Persistence.InMem
open Rebus.Transport.FileSystem
open Rebus.Persistence.FileSystem


let infof (format: Printf.StringFormat<_>) arg = sprintf format arg |> Log.Logger.Information |> ignore
let info s = s |> Log.Logger.Information |> ignore

type TransportType =
    | Memory
    | FileSystem
    | Azure

[<CLIMutable>]
type BusConfig = {
    MainQueue: string;   ErrorQueue: string
    UseAuditTrail: bool; AuditQueue: string
    UseSimpleRetry: bool; SimpleRetryCount: int
    TransportType: TransportType
    ConnectionString: string; SagaConnectionString: string
    FileSystemPath: string
} with
    static member Default = {
        MainQueue = "TheQueueForTheBus"
        ErrorQueue = SimpleRetryStrategySettings.DefaultErrorQueueName
        UseAuditTrail = false
        AuditQueue = "Audit"
        UseSimpleRetry = true
        SimpleRetryCount =  SimpleRetryStrategySettings.DefaultNumberOfDeliveryAttempts
        TransportType = Memory
        ConnectionString = ""
        SagaConnectionString = ""
        FileSystemPath = "c:/rebus"
    }
    static member FromConfig (section: IConfigurationSection) =
        // TODO: implement this.
        BusConfig.Default

let configureServices(s: ServiceCollection) =
    ()

let configureRebus (bc: BusConfig) (c: RebusConfigurer) =
    let errorQ = bc.ErrorQueue
    let maxDeliveryAttempts = if bc.UseSimpleRetry then bc.SimpleRetryCount else 1
    let secondLevelRetriesEnabled = false
    let errorDetailsHeaderMaxLength = Int32.MaxValue
    let errorTrackingMaxAgeMinutes = SimpleRetryStrategySettings.DefaultErrorTrackingMaxAgeMinutes

    c
        .Logging(fun x -> x.Serilog())
        .Options(fun x ->
            x.SimpleRetryStrategy
                (
                    errorQ,
                    maxDeliveryAttempts,
                    secondLevelRetriesEnabled,
                    errorDetailsHeaderMaxLength,
                    errorTrackingMaxAgeMinutes
                ))
        |> ignore

    if bc.UseAuditTrail then c.Options (fun x -> x.EnableMessageAuditing(bc.AuditQueue)) |> ignore

    match bc.TransportType with
    | Memory ->
        info "The bus is running in memory."
        c.Transport (fun x -> x.UseInMemoryTransport(InMemNetwork(), bc.MainQueue)) |> ignore
        c.Subscriptions (fun x -> x.StoreInMemory()) |> ignore
        c.Sagas (fun x -> x.StoreInMemory()) |> ignore
    | FileSystem ->
        infof "The bus is using the file system under '%s'." bc.FileSystemPath
        c.Transport (fun x -> x.UseFileSystem(bc.FileSystemPath, bc.MainQueue)) |> ignore
        c.Subscriptions (fun x -> x.UseJsonFile(System.IO.Path.Combine(bc.FileSystemPath, "subscriptions.json"))) |> ignore
        c.Sagas (fun x -> x.StoreInMemory()) |> ignore
    | Azure ->
        info "The bus is running in Azure."
        c.Transport (fun x -> x.UseAzureServiceBus(bc.ConnectionString, bc.MainQueue).AutomaticallyRenewPeekLock() |> ignore) |> ignore
        c.Sagas (fun x -> x.StoreInSqlServer(bc.SagaConnectionString, "Sagas", "SagaIndexes")) |> ignore
    |> ignore

    c

let subscribe (b: IBus) =
    b

type OurRebusBus() as this =
    let mutable provider: ServiceProvider  = null
    let mutable bus: IBus  = null
    do
        Log.Logger <- LoggerConfiguration().WriteTo.RollingFile("the-bus-man-{Date}.log").CreateLogger()
        info "Starting the bus."
        Log.CloseAndFlush()

        info "Reading configuration."
        let cfg = ConfigurationBuilder().AddJsonFile("appsettings.json", false).Build()

        info "Configuring logging from configuration."
        Log.Logger <- LoggerConfiguration().ReadFrom.Configuration(cfg).CreateLogger()

        info "Reading bus configuration."
        let busConfig = cfg.GetSection("Bus") |> BusConfig.FromConfig

        info "Building the service collection."
        let services = ServiceCollection()
        services.AddSingleton<IConfiguration>(cfg) |> ignore
        configureServices services |> ignore

        services.AddRebus
            (fun x ->
                    info "Configuring the bus."
                    configureRebus busConfig x
            ) |> ignore

        provider <- services.BuildServiceProvider()
        this.Provider.UseRebus
            (
                Action<IBus>
                    (fun x ->
                         info "Configuring subscriptions."
                         subscribe x |> ignore
                         bus <- x
                    )
            ) |> ignore
        ()

    interface IDisposable with
        member this.Dispose() =
            printfn "Disposing - tchau!"
            if this.Bus <> null then this.Bus.Dispose()
            if this.Provider <> null then this.Provider.Dispose()

    member this.Provider with get (): ServiceProvider = provider
    member this.Bus with get (): IBus = bus

[<EntryPoint>]
let main argv =
    Log.Logger <- LoggerConfiguration().WriteTo.ColoredConsole().CreateLogger(); // Preliminary logger for any early failures.
    ServiceHost.Run(ServiceConfiguration().Add("HereIsTheBus", fun () -> (new OurRebusBus() :> IDisposable)))
    0 // Happy shutdowns!
