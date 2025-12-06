using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using CargaCotizaciones;

var host = new HostBuilder()
    .ConfigureFunctionsWorkerDefaults()
    .ConfigureServices(services =>
    {
        services.AddApplicationInsightsTelemetryWorkerService();
        services.ConfigureFunctionsApplicationInsights();

        // 👇 Registramos la función como servicio para poder obtenerla desde DI
        services.AddSingleton<CargaCotizacion>();
    })
    .Build();

// 🧪 EJECUCIÓN MANUAL SOLO EN DEBUG
#if DEBUG
using (var scope = host.Services.CreateScope())
{
    var funcion = scope.ServiceProvider.GetRequiredService<CargaCotizacion>();

    // 👉 Acá ponés el breakpoint, o dentro de EjecutarCargaCotizaciones / donde quieras
    funcion.RunLocal();
}
#endif

// 👇 Esto deja corriendo el runtime normalmente (para el Timer, HTTP, etc.)
host.Run();
