using System;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Microsoft.Data.SqlClient;
using CargaCotizaciones.Model;
using System.Configuration;
using System.Net;
using Newtonsoft.Json.Linq;
using HtmlAgilityPack;
using ScrapySharp.Extensions;
using System.Collections;
using System.Net.Http;
using System.Threading.Tasks;

namespace CargaCotizaciones
{
    public class CargaCotizacion
    {
        private readonly ILogger _logger;

        public CargaCotizacion(ILoggerFactory loggerFactory)
        {
            _logger = loggerFactory.CreateLogger<CargaCotizacion>();
        }



        //0 24 14 * * *
        // 0 */5 * * * *

        [Function("CargaCotizaciones")]
        public void Run([TimerTrigger("0 24 14 * * *")] TimerInfo myTimer)
        {


            if (myTimer.ScheduleStatus is not null)
            {
                _logger.LogInformation($"Next timer schedule at: {myTimer.ScheduleStatus.Next}");
            }


            try
            {
                //conexion
                string connectionString = Environment.GetEnvironmentVariable("SQLCONNSTR_SQLconnectionString", EnvironmentVariableTarget.Process);
                //string connectionString = Environment.GetEnvironmentVariable("SQLconnectionString");

                //_logger.LogInformation(connectionString);

                using var connection = new SqlConnection(connectionString);

                

                connection.Open();

                

                var sqlConsultaActivos = @"SELECT A.Id ASSETID, Symbol, AT.NAME ASSETTYPE
                                            FROM ASSETS A INNER JOIN ASSETTYPES 
                                            AT ON AT.ID = A.ASSETTYPEID
                                            WHERE A.NAME  <> 'Dolar Estadounidense' AND A.ID = 75
                                            ORDER BY A.ID ASC;";

                SqlCommand cmdActivos = null;

               

                cmdActivos = new SqlCommand(sqlConsultaActivos, connection);

                SqlDataReader reader = cmdActivos.ExecuteReader();

                List<Activo> ListaActivos = new List<Activo>();


                while (reader.Read())
                {
                    Activo activo = new Activo();
                    activo.IdActivo = (int)reader["ASSETID"];
                    activo.Simbolo = (string)reader["Symbol"];
                    activo.TipoActivo = (string)reader["ASSETTYPE"];
                    ListaActivos.Add(activo);
                }

                reader.Close();


                var consultaDolar = @"SELECT A.Id ASSETID, Symbol, AT.NAME ASSETTYPE
                                            FROM ASSETS A INNER JOIN ASSETTYPES 
                                            AT ON AT.ID = A.ASSETTYPEID
                                            WHERE A.NAME  = 'Dolar Estadounidense'";

                SqlCommand cmdDolar = null;

                cmdDolar = new SqlCommand(consultaDolar, connection);

                SqlDataReader readerDolar = cmdDolar.ExecuteReader();
                
                Activo dolar = new Activo();

                while (readerDolar.Read())
                {
                    dolar.IdActivo = (int)readerDolar["ASSETID"];
                    dolar.Simbolo = (string)readerDolar["Symbol"];
                    dolar.TipoActivo = (string)readerDolar["ASSETTYPE"];
                }
                reader.Close();


                

                int contCotiz = 0;


                //datos api cripto

                string apiKey = "3a299154-851c-4a53-96d9-9ea65ea9abf7";

                client.DefaultRequestHeaders.Add("X-CMC_PRO_API_KEY", apiKey);

                foreach (Activo mon2 in ListaActivos)
                {
                    //_logger.LogInformation(mon2.ToString());

                    if (dolar != mon2)
                    {
                        UpdateCotizacionesGral(dolar, mon2, contCotiz);
                    }
                    
                }

                _logger.LogInformation($"Cotizaciones cargadas correctamente a las : {DateTime.Now}");
                
            }
            catch (Exception Ex)
            {

                Exception Excepcion = new Exception("Error al recuperar las cotizaciones", Ex);

                _logger.LogInformation($"Error al cargar cotizaciones a las: {DateTime.Now}");
                _logger.LogInformation(Ex.ToString());

            }

        }

        private int UpdateCotizacionesGral(Activo mon1, Activo mon2, int contCotiz)
        {
            string par = mon1.Simbolo + mon2.Simbolo;

            

            if (mon2.TipoActivo != "Moneda" && mon2.TipoActivo != "Criptomoneda")
            {
                par = mon2.Simbolo;
                insertCotizaciones(mon1.IdActivo.ToString(), mon2.IdActivo.ToString(), par, contCotiz, mon2.TipoActivo);
            }
            else
            {
                
                if (par == "USDARS")
                {
                    insertCotizaciones(mon1.IdActivo.ToString(), mon2.IdActivo.ToString(), par + "B", 0, mon2.TipoActivo);
                    insertCotizaciones(mon1.IdActivo.ToString(), mon2.IdActivo.ToString(), par + "BO", 0, mon2.TipoActivo);
                    insertCotizaciones(mon1.IdActivo.ToString(), mon2.IdActivo.ToString(), par + "T", 0, mon2.TipoActivo);
                }
                else if (mon2.TipoActivo == "Criptomoneda")
                {
                    contCotiz++;
                    par = mon2.Simbolo;
                    insertCotizaciones(mon1.IdActivo.ToString(), mon2.IdActivo.ToString(), par, contCotiz, mon2.TipoActivo);
                }
                else
                {
                    contCotiz++;
                    par = mon2.Simbolo + mon1.Simbolo;
                    insertCotizaciones(mon1.IdActivo.ToString(), mon2.IdActivo.ToString(), par, contCotiz, mon2.TipoActivo);
                }
            }

            return contCotiz;
        }

        private  void insertCotizaciones(string idMon1, string idMon2, string par, int contCotiz, string tipoActivo)
        {


            try
            {

                // conexion
                 string connectionString = Environment.GetEnvironmentVariable("SQLCONNSTR_SQLconnectionString", EnvironmentVariableTarget.Process);
                //string connectionString = Environment.GetEnvironmentVariable("SQLconnectionString");

                using var connection = new SqlConnection(connectionString);

                connection.Open();




                string valorCotiz;
                if (tipoActivo == "Moneda" || tipoActivo == "Accion USA")
                {
                    valorCotiz = checkCotizacion(par, contCotiz);
                }
                else if (tipoActivo == "Criptomoneda")
                {
                    valorCotiz = checkCotizacionCripto(par, contCotiz);
                }
                else
                {
                    valorCotiz = checkCotizacionScrap(par, tipoActivo);
                }

                string tipo;

                if (valorCotiz != null && valorCotiz != "0.00")
                {
                    SqlCommand insertSQL = null;

                    if (par == "USDARST")
                    {
                        tipo = "TARJETA";
                    }
                    else if (par == "USDARSB")
                    {
                        tipo = "BLUE";
                    }
                    else if (par == "USDARSBO")
                    {
                        tipo = "BOLSA";
                    }
                    else
                    {
                        tipo = "NA";
                    }

                    string sqlQuery = "INSERT INTO ASSETQUOTES (ASSETID, DATE, TYPE, VALUE) VALUES ('@ID2', DATEADD(DAY, 0, DATEDIFF(DAY, 0, GETDATE())), '@TIPO', @VALOR)";

                    sqlQuery = sqlQuery.Replace("@ID2", idMon2);
                    sqlQuery = sqlQuery.Replace("@TIPO", tipo);

                    if (tipoActivo == "FCI" || tipoActivo == "Bono" || tipoActivo == "CEDEAR" ||
                        tipoActivo == "Accion Argentina" || tipoActivo == "Obligacion Negociable")
                    {
                        string sqlValor = "1/(" + valorCotiz + "/ (SELECT VALUE FROM ASSETQUOTES WHERE TYPE = 'BLUE' AND " +
                            "DATE = (SELECT MAX(DATE) FROM ASSETQUOTES WHERE TYPE = 'BOLSA')))";
                        sqlQuery = sqlQuery.Replace("@VALOR", sqlValor);

                    }
                    else
                    {
                        sqlQuery = sqlQuery.Replace("@VALOR", valorCotiz.Replace(",", "."));
                    }




                    insertSQL = new SqlCommand(sqlQuery, connection);

                    insertSQL.ExecuteNonQuery();
                }
            }
            catch (Exception Ex)
            {

                Exception Excepcion = new Exception("Error al recuperar las cotizaciones", Ex);
                
            }
        }
        private static readonly HttpClient client = new HttpClient();

        public string checkCotizacionCripto(string simbolo, int contCotiz)
        {
            string cotiz;
            string convertToCurrency = "USD";

            

            string currencyPair = simbolo;

            var url = $"https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest?symbol={simbolo}&convert={convertToCurrency}";

            

            // Realizar la solicitud GET
            HttpResponseMessage response = client.GetAsync(url).Result;
            response.EnsureSuccessStatusCode();

            // Leer la respuesta como string
            string responseBody = response.Content.ReadAsStringAsync().Result;

            // Parsear el JSON usando Newtonsoft.Json
            JObject json = JObject.Parse(responseBody);
            decimal price = json["data"][simbolo]["quote"][convertToCurrency]["price"].Value<decimal>();

            return Convert.ToString(1 / price);
        }

        public string checkCotizacion(string par, int contCotiz)
        {
            string cotiz;
            string url;
            if (par == "USDARSB")
            {
                url = $"https://dolarapi.com/v1/dolares/blue";
            }
            else if (par == "USDARST")
            {
                url = $"https://dolarapi.com/v1/dolares/tarjeta";
            }
            else if (par == "USDARSBO")
            {
                url = $"https://dolarapi.com/v1/dolares/bolsa";
            }
            else
            {
                string apiKey;
                if (contCotiz <= 25)
                {
                    apiKey = "VMFI7FT36ZW14QLB";
                }
                else if (contCotiz <= 50)
                {
                    apiKey = "FDYDOY4B5LA56344";
                }
                else
                {
                    apiKey = "CUKHS041RB7MRZSV";
                }


                string currencyPair = par;

                url = $"https://www.alphavantage.co/query?function=CURRENCY_EXCHANGE_RATE&from_currency={currencyPair.Substring(0, 3)}&to_currency={currencyPair.Substring(3)}&apikey={apiKey}";




            }

            try
            {
                using (WebClient wc = new WebClient())
                {

                    string json = wc.DownloadString(url);
                    JObject data = JObject.Parse(json);

                    if (data["Error Message"] != null)
                    {
                        //Console.WriteLine(data["Error Message"]);
                        cotiz = null;
                    }
                    else
                    {
                        if (par == "USDARSB" | par == "USDARSBO")
                        {

                            decimal cot1 = (Convert.ToDecimal(data["venta"]));
                            decimal cot2 = (Convert.ToDecimal(data["compra"]));
                            cotiz = Convert.ToString((cot1 + cot2) / 2);
                        }
                        else if (par == "USDARST")
                        {
                            cotiz = Convert.ToString(data["venta"]);
                        }
                        else
                        {
                            string check = Convert.ToString(data["Information"]);

                            if (!check.Contains("limit is 25"))
                            {

                                decimal cotizDec = Convert.ToDecimal(data["Realtime Currency Exchange Rate"]["5. Exchange Rate"]);
                                cotiz = Convert.ToString(1 / cotizDec);
                            }
                            else
                            {
                                cotiz = null;
                            }
                        }


                    }

                }

            }

            catch (WebException ex)

            {
                // Si hay un error de red, muestra el mensaje de error
                Console.WriteLine("Error de red: " + ex.Message);
                cotiz = null;

            }

            catch (Exception ex)
            {
                // Si hay otro tipo de error, muestra el mensaje de error
                Console.WriteLine("Error: " + ex.Message);
                cotiz = null;
            }

            return cotiz;

        }

        public string checkCotizacionScrap(string simbolo, string tipo)
        {
            string cotiz = "0";

            if (tipo == "FCI")
            {
                HtmlWeb oWeb = new HtmlWeb();
                HtmlDocument doc = oWeb.Load("https://bullmarketbrokers.com/Information/FundData?ticker=" + simbolo);

                //HtmlNode Body = doc.DocumentNode.CssSelect("body").First();
                //string sBody = Body.InnerHtml;

                //Console.WriteLine("https://bullmarketbrokers.com/Information/FundData?ticker=" + simbolo);


                var nodo1 = doc.DocumentNode.CssSelect(".table-hover").Last();

                var nodo2 = nodo1.CssSelect("tr").First();

                var nodo3 = nodo2.CssSelect("td").Last();


                cotiz = nodo3.InnerHtml;

                cotiz = cotiz.Replace(".", "").Replace("$", "").Replace(" ", "");

                cotiz = cotiz.Replace(",", ".");


            }
            else if (tipo == "Bono" || tipo == "Obligacion Negociable")
            {
                HtmlWeb oWeb = new HtmlWeb();
                HtmlDocument doc = oWeb.Load("https://www.allaria.com.ar/Bono/Especie/" + simbolo);

                HtmlNode Body = doc.DocumentNode.CssSelect("body").First();
                //string sBody = Body.InnerHtml;

                var nodo1 = doc.DocumentNode.CssSelect(".float-left").First();
                cotiz = nodo1.InnerHtml;
                char delimiter = ',';
                string[] substrings = cotiz.Split(delimiter);
                cotiz = substrings[0].Replace("$", "").Replace(".", "");
            }
            else if (tipo == "CEDEAR" || tipo == "Accion Argentina")
            {
                HtmlWeb oWeb = new HtmlWeb();
                HtmlDocument doc = oWeb.Load("https://iol.invertironline.com/titulo/cotizacion/BCBA/" + simbolo);

                //HtmlNode Body = doc.DocumentNode.CssSelect("body").First();
                //string sBody = Body.InnerHtml;

                var nodo1 = doc.DocumentNode.CssSelect("span[data-field='UltimoPrecio']").First();
                cotiz = nodo1.InnerHtml;
                cotiz = cotiz.Replace(".", "").Replace("$", "").Replace(" ", "");
                cotiz = cotiz.Replace(",", ".");
            }

            //decimal transformacion = 1 / Convert.ToDecimal(cotiz);
            //cotiz = transformacion.ToString();
            //cotiz = cotiz.Replace(",", ".");

            return cotiz;
        }

    }
}
