using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using HtmlAgilityPack;
using ScrapySharp.Extensions;
using CargaCotizaciones.Model;

namespace CargaCotizaciones
{
    /// <summary>
    /// Constantes centralizadas para evitar strings y valores "quemados" dispersos.
    /// TODO futuro: mover a archivo separado o a configuración/env vars.
    /// </summary>
    public static class CotizacionConsts
    {
        // --- Tipos de activos ---
        public const string AssetTypeCurrency = "Moneda";
        public const string AssetTypeCrypto = "Criptomoneda";
        public const string AssetTypeStockUSA = "Accion USA";
        public const string AssetTypeStockAR = "Accion Argentina";
        public const string AssetTypeBond = "Bono";
        public const string AssetTypeON = "Obligacion Negociable";
        public const string AssetTypeFCI = "FCI";
        public const string AssetTypeCedear = "CEDEAR";

        // --- Nombre de activo para el dólar ---
        public const string DolarName = "Dolar Estadounidense";

        // --- Tipos de cotización (campo TYPE en ASSETQUOTES) ---
        public const string QuoteTypeBlue = "BLUE";
        public const string QuoteTypeCard = "TARJETA";
        public const string QuoteTypeMEP = "BOLSA";
        public const string QuoteTypeNA = "NA";

        // --- Par base para pesos argentinos vs USD ---
        public const string UsdArsPar = "USDARS";
        public const string UsdArsBlueSuffix = "B";
        public const string UsdArsCardSuffix = "T";
        public const string UsdArsMepSuffix = "BO";

        // --- URLs base de APIs externas ---
        public const string ApiDolarBase = "https://dolarapi.com/v1/dolares/";
        public const string ApiCMCBase = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest";
        public const string ApiAlphaVantageBase = "https://www.alphavantage.co/query";
        public const string ApiBmbFciBase = "https://bullmarketbrokers.com/Cotizaciones/Fondos/";
        public const string ApiAllariaBondBase = "https://www.allaria.com.ar/Bono/Especie/";
        public const string ApiIolStockArBase = "https://iol.invertironline.com/titulo/cotizacion/BCBA/";

        // --- API Keys (por ahora hardcodeadas en este archivo) ---
        // OJO: en producción es recomendable mover esto a variables de entorno.
        public const string CmcApiKey = "3a299154-851c-4a53-96d9-9ea65ea9abf7";

        public const string AlphaVantageApiKey1 = "VMFI7FT36ZW14QLB";
        public const string AlphaVantageApiKey2 = "FDYDOY4B5LA56344";
        public const string AlphaVantageApiKey3 = "CUKHS041RB7MRZSV";

        // Límites de llamadas para repartir entre API keys
        public const int AlphaVantageFirstKeyLimit = 25;
        public const int AlphaVantageSecondKeyLimit = 50;

        // --- Otros valores comunes ---
        public const string CurrencyUSD = "USD";

        public const string AlphaFnCurrencyExchangeRate = "CURRENCY_EXCHANGE_RATE";
        public const string AlphaFnGlobalQuote = "GLOBAL_QUOTE";

        // --- Selectores de scraping HTML ---
        public const string SelectorFciPrice = "#displayPrice";
        public const string SelectorBondPriceContainer = ".float-left";
        public const string SelectorStockArPrice = "span[data-field='UltimoPrecio']";

        // --- Nombre de la variable de entorno de la connection string SQL ---
        public const string SqlConnectionStringEnvVar = "SQLCONNSTR_SQLconnectionString";

        // --- Cron para el TimerTrigger ---
        public const string CronCargaCotizaciones = "0 27 14 * * *";
    }

    public class CargaCotizacion
    {
        private readonly ILogger<CargaCotizacion> _logger;

        // HttpClient reutilizable
        private static readonly HttpClient _httpClient = new HttpClient();

        // Connection string (se lee una vez)
        private readonly string _connectionString;

        public CargaCotizacion(ILoggerFactory loggerFactory)
        {
            _logger = loggerFactory.CreateLogger<CargaCotizacion>();

            _connectionString = Environment.GetEnvironmentVariable(
                                    CotizacionConsts.SqlConnectionStringEnvVar,
                                    EnvironmentVariableTarget.Process
                                )
                                ?? throw new InvalidOperationException(
                                    $"No se encontró la cadena de conexión '{CotizacionConsts.SqlConnectionStringEnvVar}' en las variables de entorno."
                                );
        }

        // 🔔 Función real, llamada por el Timer en Azure
        [Function("CargaCotizaciones")]
        public void Run([TimerTrigger(CotizacionConsts.CronCargaCotizaciones)] TimerInfo timer)
        {
            if (timer?.ScheduleStatus is not null)
            {
                _logger.LogInformation($"Next timer schedule at: {timer.ScheduleStatus.Next}");
            }

            _logger.LogInformation($"CargaCotizaciones iniciada a las: {DateTime.Now}");

            EjecutarCargaCotizaciones();

            _logger.LogInformation($"CargaCotizaciones finalizada a las: {DateTime.Now}");
        }

        // 🧪 Función para debug local (podés llamarla desde Program.cs)
        public void RunLocal()
        {
            _logger.LogInformation($"[DEBUG LOCAL] CargaCotizaciones ejecutada manualmente a las: {DateTime.Now}");
            EjecutarCargaCotizaciones();
        }

        /// <summary>
        /// Lógica principal: leer activos, obtener cotizaciones y grabar en la base.
        /// </summary>
        private void EjecutarCargaCotizaciones()
        {
            try
            {
                using var connection = new SqlConnection(_connectionString);
                connection.Open();

                // 1) Obtenemos todos los activos (excepto el dólar)
                var activos = ObtenerActivos(connection);

                // 2) Obtenemos el activo que representa el dólar estadounidense
                var dolar = ObtenerActivoDolar(connection);

                if (dolar == null)
                {
                    _logger.LogWarning($"No se encontró el activo '{CotizacionConsts.DolarName}'. No se pueden cargar cotizaciones.");
                    return;
                }

                int contCotiz = 0;

                // 3) Recorremos los activos y actualizamos cotizaciones
                foreach (var activo in activos)
                {
                    contCotiz = UpdateCotizacionesGral(connection, dolar, activo, contCotiz);
                }

                _logger.LogInformation($"Cotizaciones cargadas correctamente a las: {DateTime.Now}");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al ejecutar la carga de cotizaciones.");
            }
        }

        #region Acceso a datos

        private static List<Activo> ObtenerActivos(SqlConnection connection)
        {
            const string sql = @"
                SELECT A.Id AS ASSETID, Symbol, AT.NAME AS ASSETTYPE
                FROM ASSETS A 
                INNER JOIN ASSETTYPES AT ON AT.ID = A.ASSETTYPEID
                WHERE A.NAME <> @DolarName
                ORDER BY A.ID ASC;";

            var lista = new List<Activo>();

            using var cmd = new SqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@DolarName", CotizacionConsts.DolarName);
            using var reader = cmd.ExecuteReader();

            while (reader.Read())
            {
                var activo = new Activo
                {
                    IdActivo = (int)reader["ASSETID"],
                    Simbolo = (string)reader["Symbol"],
                    TipoActivo = (string)reader["ASSETTYPE"]
                };

                lista.Add(activo);
            }

            return lista;
        }

        private static Activo ObtenerActivoDolar(SqlConnection connection)
        {
            const string sql = @"
                SELECT A.Id AS ASSETID, Symbol, AT.NAME AS ASSETTYPE
                FROM ASSETS A 
                INNER JOIN ASSETTYPES AT ON AT.ID = A.ASSETTYPEID
                WHERE A.NAME = @DolarName;";

            using var cmd = new SqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@DolarName", CotizacionConsts.DolarName);
            using var reader = cmd.ExecuteReader();

            Activo? dolar = null;

            if (reader.Read())
            {
                dolar = new Activo
                {
                    IdActivo = (int)reader["ASSETID"],
                    Simbolo = (string)reader["Symbol"],
                    TipoActivo = (string)reader["ASSETTYPE"]
                };
            }

            return dolar;
        }

        #endregion

        #region Lógica de armado de pares y tipos

        /// <summary>
        /// Decide el par y el tipo de consulta según el tipo de activo
        /// y delega en InsertCotizacion.
        /// </summary>
        private int UpdateCotizacionesGral(SqlConnection connection, Activo dolar, Activo activo, int contCotiz)
        {
            var parBase = CotizacionConsts.UsdArsPar; // "USDARS"
            var par = parBase + activo.Simbolo;

            if (activo.TipoActivo != CotizacionConsts.AssetTypeCurrency &&
                activo.TipoActivo != CotizacionConsts.AssetTypeCrypto)
            {
                // Acciones AR, bonos, FCI, CEDEAR, ON, etc.
                par = activo.Simbolo;
                InsertCotizacion(connection, dolar.IdActivo.ToString(), activo.IdActivo.ToString(), par, contCotiz, activo.TipoActivo);
            }
            else
            {
                // Monedas / Cripto
                if (par == CotizacionConsts.UsdArsPar)
                {
                    // Distintos tipos de dólar oficial/blue/MEP/tarjeta
                    InsertCotizacion(
                        connection,
                        dolar.IdActivo.ToString(),
                        activo.IdActivo.ToString(),
                        par + CotizacionConsts.UsdArsBlueSuffix,
                        0,
                        activo.TipoActivo);

                    InsertCotizacion(
                        connection,
                        dolar.IdActivo.ToString(),
                        activo.IdActivo.ToString(),
                        par + CotizacionConsts.UsdArsMepSuffix,
                        0,
                        activo.TipoActivo);

                    InsertCotizacion(
                        connection,
                        dolar.IdActivo.ToString(),
                        activo.IdActivo.ToString(),
                        par + CotizacionConsts.UsdArsCardSuffix,
                        0,
                        activo.TipoActivo);
                }
                else if (activo.TipoActivo == CotizacionConsts.AssetTypeCrypto ||
                         activo.TipoActivo == CotizacionConsts.AssetTypeStockUSA)
                {
                    contCotiz++;
                    par = activo.Simbolo;
                    InsertCotizacion(connection, dolar.IdActivo.ToString(), activo.IdActivo.ToString(), par, contCotiz, activo.TipoActivo);
                }
                else
                {
                    contCotiz++;
                    par = activo.Simbolo + dolar.Simbolo;
                    InsertCotizacion(connection, dolar.IdActivo.ToString(), activo.IdActivo.ToString(), par, contCotiz, activo.TipoActivo);
                }
            }

            return contCotiz;
        }

        #endregion

        #region Inserción de cotizaciones en BD

        private static readonly Dictionary<string, string> TipoPorPar = new()
        {
            { CotizacionConsts.UsdArsPar + CotizacionConsts.UsdArsCardSuffix, CotizacionConsts.QuoteTypeCard }, // USDARST
            { CotizacionConsts.UsdArsPar + CotizacionConsts.UsdArsBlueSuffix, CotizacionConsts.QuoteTypeBlue }, // USDARSB
            { CotizacionConsts.UsdArsPar + CotizacionConsts.UsdArsMepSuffix,  CotizacionConsts.QuoteTypeMEP }   // USDARSBO
        };

        private void InsertCotizacion(
            SqlConnection connection,
            string idMon1,
            string idMon2,
            string par,
            int contCotiz,
            string tipoActivo)
        {
            try
            {
                var valorCotiz = ObtenerValorCotizacion(par, contCotiz, tipoActivo);

                if (string.IsNullOrWhiteSpace(valorCotiz) || valorCotiz == "0.00")
                {
                    _logger.LogWarning($"No se obtuvo valor de cotización para {par} ({tipoActivo}).");
                    return;
                }

                if (!decimal.TryParse(
                        valorCotiz,
                        System.Globalization.NumberStyles.Any,
                        System.Globalization.CultureInfo.InvariantCulture,
                        out var valorDecimal))
                {
                    _logger.LogWarning($"No fue posible parsear el valor de cotización '{valorCotiz}' para {par}.");
                    return;
                }

                var tipo = ObtenerTipoCotizacionDesdePar(par);

                // Para FCI / Bonos / CEDEAR / Acciones AR / ON, se convierte usando dólar blue/boca.
                var requiereConversionBlue =
                    tipoActivo == CotizacionConsts.AssetTypeFCI ||
                    tipoActivo == CotizacionConsts.AssetTypeBond ||
                    tipoActivo == CotizacionConsts.AssetTypeCedear ||
                    tipoActivo == CotizacionConsts.AssetTypeStockAR ||
                    tipoActivo == CotizacionConsts.AssetTypeON;

                using var cmd = new SqlCommand
                {
                    Connection = connection,
                    CommandText = requiereConversionBlue
                        ? @"
INSERT INTO ASSETQUOTES (ASSETID, DATE, TYPE, VALUE)
VALUES (
    @AssetId,
    DATEADD(DAY, 0, DATEDIFF(DAY, 0, GETDATE())),
    @Tipo,
    1 / (@Valor / (SELECT TOP(1) VALUE 
                   FROM ASSETQUOTES 
                   WHERE TYPE = @TipoBlue 
                   ORDER BY DATE DESC))
);"
                        : @"
INSERT INTO ASSETQUOTES (ASSETID, DATE, TYPE, VALUE)
VALUES (
    @AssetId,
    DATEADD(DAY, 0, DATEDIFF(DAY, 0, GETDATE())),
    @Tipo,
    @Valor
);"
                };

                cmd.Parameters.AddWithValue("@AssetId", idMon2);
                cmd.Parameters.AddWithValue("@Tipo", tipo);
                cmd.Parameters.AddWithValue("@Valor", valorDecimal);

                if (requiereConversionBlue)
                {
                    cmd.Parameters.AddWithValue("@TipoBlue", CotizacionConsts.QuoteTypeBlue);
                }

                cmd.ExecuteNonQuery();

                _logger.LogInformation($"Insertada cotización {par} ({tipoActivo}) - Tipo: {tipo} - Valor: {valorDecimal}");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error al insertar la cotización para el par {par} ({tipoActivo}).");
            }
        }

        private static string ObtenerTipoCotizacionDesdePar(string par) =>
            TipoPorPar.TryGetValue(par, out var tipo) ? tipo : CotizacionConsts.QuoteTypeNA;

        private string ObtenerValorCotizacion(string par, int contCotiz, string tipoActivo)
        {
            return tipoActivo switch
            {
                var t when t == CotizacionConsts.AssetTypeCurrency
                    => CheckCotizacionMoneda(par, contCotiz),

                var t when t == CotizacionConsts.AssetTypeStockUSA
                    => CheckCotizacionAccionUSA(par, contCotiz),

                var t when t == CotizacionConsts.AssetTypeCrypto
                    => CheckCotizacionCripto(par, contCotiz),

                _ => CheckCotizacionScrap(par, tipoActivo)
            };
        }

        #endregion

        #region Cotizaciones: APIs y Scraping

        public string CheckCotizacionCripto(string simbolo, int contCotiz)
        {
            var url =
                $"{CotizacionConsts.ApiCMCBase}?symbol={simbolo}&convert={CotizacionConsts.CurrencyUSD}";

            var request = new HttpRequestMessage(HttpMethod.Get, url);
            request.Headers.Remove("X-CMC_PRO_API_KEY");
            request.Headers.Add("X-CMC_PRO_API_KEY", CotizacionConsts.CmcApiKey);

            var response = _httpClient.Send(request);
            response.EnsureSuccessStatusCode();

            var responseBody = response.Content.ReadAsStringAsync().Result;

            var json = JObject.Parse(responseBody);
            var price = json["data"]?[simbolo]?["quote"]?[CotizacionConsts.CurrencyUSD]?["price"]?.Value<decimal>() ?? 0m;

            if (price <= 0)
                return null;

            return (1 / price).ToString(System.Globalization.CultureInfo.InvariantCulture);
        }

        public string CheckCotizacionMoneda(string par, int contCotiz)
        {
            string cotiz;
            string url;

            if (par == CotizacionConsts.UsdArsPar + CotizacionConsts.UsdArsBlueSuffix)
            {
                url = CotizacionConsts.ApiDolarBase + "blue";
            }
            else if (par == CotizacionConsts.UsdArsPar + CotizacionConsts.UsdArsCardSuffix)
            {
                url = CotizacionConsts.ApiDolarBase + "tarjeta";
            }
            else if (par == CotizacionConsts.UsdArsPar + CotizacionConsts.UsdArsMepSuffix)
            {
                url = CotizacionConsts.ApiDolarBase + "bolsa";
            }
            else
            {
                var apiKey = SeleccionarApiKeyAlphavantage(contCotiz);

                var fromCurrency = par[..3];
                var toCurrency = par[3..];

                url =
                    $"{CotizacionConsts.ApiAlphaVantageBase}?function={CotizacionConsts.AlphaFnCurrencyExchangeRate}" +
                    $"&from_currency={fromCurrency}&to_currency={toCurrency}&apikey={apiKey}";
            }

            try
            {
                using var wc = new WebClient();
                var json = wc.DownloadString(url);
                var data = JObject.Parse(json);

                if (data["Error Message"] != null)
                {
                    return null;
                }

                if (par == CotizacionConsts.UsdArsPar + CotizacionConsts.UsdArsBlueSuffix ||
                    par == CotizacionConsts.UsdArsPar + CotizacionConsts.UsdArsMepSuffix)
                {
                    var venta = Convert.ToDecimal(data["venta"]);
                    var compra = Convert.ToDecimal(data["compra"]);
                    cotiz = ((venta + compra) / 2m).ToString(System.Globalization.CultureInfo.InvariantCulture);
                }
                else if (par == CotizacionConsts.UsdArsPar + CotizacionConsts.UsdArsCardSuffix)
                {
                    cotiz = Convert.ToDecimal(data["venta"])
                        .ToString(System.Globalization.CultureInfo.InvariantCulture);
                }
                else
                {
                    var check = Convert.ToString(data["Information"]);

                    if (!string.IsNullOrEmpty(check) &&
                        check.Contains("limit is 25", StringComparison.OrdinalIgnoreCase))
                    {
                        return null;
                    }

                    var cotizDec = Convert.ToDecimal(
                        data["Realtime Currency Exchange Rate"]["5. Exchange Rate"]
                    );

                    cotiz = (1 / cotizDec).ToString(System.Globalization.CultureInfo.InvariantCulture);
                }

                return cotiz;
            }
            catch (WebException ex)
            {
                _logger.LogError(ex, $"Error de red al obtener cotización para {par}");
                return null;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error al obtener cotización para {par}");
                return null;
            }
        }

        public string CheckCotizacionAccionUSA(string simbolo, int contCotiz)
        {
            var apiKey = SeleccionarApiKeyAlphavantage(contCotiz);

            var url =
                $"{CotizacionConsts.ApiAlphaVantageBase}?function={CotizacionConsts.AlphaFnGlobalQuote}" +
                $"&symbol={simbolo}&apikey={apiKey}";

            try
            {
                using var wc = new WebClient();
                var json = wc.DownloadString(url);
                var data = JObject.Parse(json);

                if (data["Error Message"] != null)
                {
                    return null;
                }

                var check = Convert.ToString(data["Information"]);

                if (!string.IsNullOrEmpty(check) &&
                    check.Contains("limit is 25", StringComparison.OrdinalIgnoreCase))
                {
                    return null;
                }

                var price = Convert.ToDecimal(data["Global Quote"]["05. price"]);

                return (1 / price).ToString(System.Globalization.CultureInfo.InvariantCulture);
            }
            catch (WebException ex)
            {
                _logger.LogError(ex, $"Error de red al obtener cotización Accion USA {simbolo}");
                return null;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error al obtener cotización Accion USA {simbolo}");
                return null;
            }
        }

        public string CheckCotizacionScrap(string simbolo, string tipo)
        {
            string cotiz = null;

            try
            {
                if (tipo == CotizacionConsts.AssetTypeFCI)
                {
                    var web = new HtmlWeb();
                    var url = CotizacionConsts.ApiBmbFciBase + simbolo;
                    var doc = web.Load(url);

                    var priceNode = doc.DocumentNode.CssSelect(CotizacionConsts.SelectorFciPrice).FirstOrDefault();
                    if (priceNode == null)
                    {
                        _logger.LogWarning($"No se encontró el nodo de precio para FCI {simbolo}");
                        return null;
                    }

                    cotiz = priceNode.InnerText;

                    cotiz = cotiz
                        .Replace("ARS", "")
                        .Replace("$", "")
                        .Replace(" ", "")
                        .Replace(".", "")
                        .Replace(",", ".");
                }
                else if (tipo == CotizacionConsts.AssetTypeBond || tipo == CotizacionConsts.AssetTypeON)
                {
                    var web = new HtmlWeb();
                    var url = CotizacionConsts.ApiAllariaBondBase + simbolo;
                    var doc = web.Load(url);

                    var nodo1 = doc.DocumentNode.CssSelect(CotizacionConsts.SelectorBondPriceContainer).FirstOrDefault();
                    if (nodo1 == null)
                    {
                        _logger.LogWarning($"No se encontró el nodo de precio para Bono/ON {simbolo}");
                        return null;
                    }

                    var texto = nodo1.InnerHtml;
                    var partes = texto.Split(',');

                    if (partes.Length > 0)
                    {
                        cotiz = partes[0].Replace("$", "").Replace(".", "");
                    }
                }
                else if (tipo == CotizacionConsts.AssetTypeCedear ||
                         tipo == CotizacionConsts.AssetTypeStockAR)
                {
                    var web = new HtmlWeb();
                    var url = CotizacionConsts.ApiIolStockArBase + simbolo;
                    var doc = web.Load(url);

                    var nodo1 = doc.DocumentNode
                        .CssSelect(CotizacionConsts.SelectorStockArPrice)
                        .FirstOrDefault();

                    if (nodo1 == null)
                    {
                        _logger.LogWarning($"No se encontró el nodo de precio para CEDEAR/Acción AR {simbolo}");
                        return null;
                    }

                    cotiz = nodo1.InnerHtml;
                    cotiz = cotiz
                        .Replace(".", "")
                        .Replace("$", "")
                        .Replace(" ", "")
                        .Replace(",", ".");
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error haciendo scraping de {tipo} ({simbolo}).");
                cotiz = null;
            }

            return cotiz;
        }

        private string SeleccionarApiKeyAlphavantage(int contCotiz)
        {
            if (contCotiz <= CotizacionConsts.AlphaVantageFirstKeyLimit)
            {
                return CotizacionConsts.AlphaVantageApiKey1;
            }

            if (contCotiz <= CotizacionConsts.AlphaVantageSecondKeyLimit)
            {
                return CotizacionConsts.AlphaVantageApiKey2;
            }

            return CotizacionConsts.AlphaVantageApiKey3;
        }

        #endregion
    }
}
