# RebusFS
A basic [Rebus](https://rebus.fm/) service bus implemented in F# running in .NET Core using [Topper](https://github.com/rebus-org/Topper) with configuration to control running against memory, file system or Azure Service Bus transports.

Using Topper (which is based on TopShelf) allows us to just run and debug the bus locally as a console app, install it as a Windows Service or host it as an Azure Web Job.
