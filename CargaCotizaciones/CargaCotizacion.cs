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

namespace CargaCotizaciones
{
    public class CargaCotizacion
    {
        private readonly ILogger _logger;

        public CargaCotizacion(ILoggerFactory loggerFactory)
        {
            _logger = loggerFactory.CreateLogger<CargaCotizacion>();
        }



        //50 23 * * *

        [Function("Function1")]
        public void Run([TimerTrigger("*/1 * * * *")] TimerInfo myTimer)
        {

            //_logger.LogInformation($"C# Timer trigger function executed at: {DateTime.Now}");


            //if (myTimer.ScheduleStatus is not null)
            //{
            //    _logger.LogInformation($"Next timer schedule at: {myTimer.ScheduleStatus.Next}");
            //}


            try
            {
                // conexion
                string connectionString = Environment.GetEnvironmentVariable("SQLconnectionString");

                using var connection = new SqlConnection(connectionString);

                connection.Open();



                var sqlConsultaActivos = @"SELECT idActivo, simbolo, TA.nombre TIPOACTIVO
                                        FROM Dim_Activo A INNER JOIN Dim_Tipo_Activo 
                                        TA ON TA.idTipoActivo = A.idtipoactivo 
                                        ORDER BY ESREFERENCIA DESC, IDACTIVO ASC;";

                SqlCommand cmdActivos = null;

                cmdActivos = new SqlCommand(sqlConsultaActivos, connection);

                SqlDataReader reader = cmdActivos.ExecuteReader();

                List<Activo> ListaActivos = new List<Activo>();


                while (reader.Read())
                {
                    Activo activo = new Activo();
                    activo.IdActivo = (int)reader["idActivo"];
                    activo.Simbolo = (string)reader["Simbolo"];
                    activo.TipoActivo = (string)reader["TIPOACTIVO"];
                    ListaActivos.Add(activo);
                }

                reader.Close();

                Activo mon1 = ListaActivos.First();

                int contCotiz = 0;
                foreach (Activo mon2 in ListaActivos)
                {
                    UpdateCotizacionesGral(mon1, mon2, contCotiz);
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
                else
                {
                    contCotiz++;
                    par = mon2.Simbolo + mon1.Simbolo;
                    insertCotizaciones(mon1.IdActivo.ToString(), mon2.IdActivo.ToString(), par, contCotiz, mon2.TipoActivo);
                }
            }

            return contCotiz;
        }

        private void insertCotizaciones(string idMon1, string idMon2, string par, int contCotiz, string tipoActivo)
        {


            try
            {

                // conexion
                string connectionString = Environment.GetEnvironmentVariable("SQLconnectionString");

                using var connection = new SqlConnection(connectionString);

                connection.Open();




                string valorCotiz;
                if (tipoActivo == "Moneda" || tipoActivo == "Criptomoneda")
                {
                    if (par == "USDTUSD" || par == "DAIUSD")
                    {
                        valorCotiz = "1";
                    }
                    else
                    {
                        valorCotiz = checkCotizacion(par, contCotiz);
                    }
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

                    string sqlQuery = "INSERT INTO Cotizacion_Activo (idActivoBase, idActivoComp, idFecha, tipo, valor) VALUES ('@ID1', '@ID2', CAST(FORMAT(DATEADD(HOUR, -3,GETDATE()), 'yyyyMMdd') AS INTEGER), '@TIPO', @VALOR)";

                    sqlQuery = sqlQuery.Replace("@ID1", idMon1);
                    sqlQuery = sqlQuery.Replace("@ID2", idMon2);
                    sqlQuery = sqlQuery.Replace("@TIPO", tipo);

                    if (tipoActivo == "FCI" || tipoActivo == "Bonos" || tipoActivo == "CEDEAR" ||
                        tipoActivo == "Accion Argentina")
                    {
                        string sqlValor = "1/(" + valorCotiz + "/ (SELECT VALOR FROM Cotizacion_Activo WHERE TIPO = 'BLUE' AND " +
                            "IDFECHA = (SELECT MAX(IDFECHA) FROM Cotizacion_Activo WHERE TIPO = 'BOLSA')))";
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

                cotiz = cotiz.Replace(",", ".");


            }
            else if (tipo == "Bonos")
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
