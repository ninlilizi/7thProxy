using Caching;
using DiskQueue;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Security;
using System.Runtime.InteropServices;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Titanium.Web.Proxy;
using Titanium.Web.Proxy.EventArguments;
using Titanium.Web.Proxy.Exceptions;
using Titanium.Web.Proxy.Helpers;
using Titanium.Web.Proxy.Http;
using Titanium.Web.Proxy.Models;
using Titanium.Web.Proxy.StreamExtended.Network;
using WatsonDedupe;
using Noemax.Compression;
using SJP.DiskCache;
using System.Net.Http.Headers;

namespace NKLI.DeDupeProxy
{
    public class TitaniumController : IDisposable
    {
        private readonly SemaphoreSlim @lock = new SemaphoreSlim(1);
        private ProxyServer proxyServer;
        private ExplicitProxyEndPoint explicitEndPoint;

        public static int RemotePort_Socks;

        public static List<string> dontDecrypt;
             
        // Chunk cache
        DirectoryInfo chunkCacheDir = new DirectoryInfo(@"chunkCache");   // where to store the cache
        LfuCachePolicy<string> chunkCachePolicy = new LfuCachePolicy<string>();      // using an LRU cache policy
        const ulong chunkCacheUnitSize = 1073741824UL; // 1GB
        const ulong chunkCacheMaxSize = chunkCacheUnitSize * 10UL; // 10GB
        TimeSpan chunkCachePollingInterval = TimeSpan.FromMinutes(1);
        DiskCache<string> chunkCache;
        //

        // Chunk encoding queue
        public static IPersistentQueue chunkQueue;
        public ulong chunkLock = 0;
        public bool queueLock = false;

        struct ObjectStruct
        {
            public string URI;
            public byte[] Headers;
            public byte[] Body;

            public byte[] packed;

            public void PackStruct(string requestURI, byte[] incomingHeaders, byte[] incomingBody)
            {
                URI = requestURI;
                
                int packedlength = 12 + requestURI.Length + incomingHeaders.Length + incomingBody.Length;

                packed = new byte[packedlength];

                packed[0] = (byte)requestURI.Length;
                packed[1] = (byte)(requestURI.Length >> 8);
                packed[2] = (byte)(requestURI.Length >> 0x10);
                packed[3] = (byte)(requestURI.Length >> 0x18);

                packed[4] = (byte)incomingHeaders.Length;
                packed[5] = (byte)(incomingHeaders.Length >> 8);
                packed[6] = (byte)(incomingHeaders.Length >> 0x10);
                packed[7] = (byte)(incomingHeaders.Length >> 0x18);

                packed[8] = (byte)incomingBody.Length;
                packed[9] = (byte)(incomingBody.Length >> 8);
                packed[10] = (byte)(incomingBody.Length >> 0x10);
                packed[11] = (byte)(incomingBody.Length >> 0x18);

                Buffer.BlockCopy(Encoding.UTF8.GetBytes(URI), 0, packed, 12, requestURI.Length);
                Buffer.BlockCopy(incomingHeaders, 0, packed, 12 + requestURI.Length, incomingHeaders.Length);
                if (incomingBody.Length != 0) Buffer.BlockCopy(incomingBody, 0, packed, 12 + requestURI.Length + incomingHeaders.Length, incomingBody.Length);
            }

            public void UnPackStruct(byte[] packed)
            {
                int URILength = BitConverter.ToInt32(packed, 0);
                int HeaderLength = BitConverter.ToInt32(packed, 4);
                int BodyLength = BitConverter.ToInt32(packed, 8);

                Headers = new byte[HeaderLength];
                Body = new byte[BodyLength];

                URI = Encoding.UTF8.GetString(packed, 12, URILength);

                Buffer.BlockCopy(packed, 12 + URILength, Headers, 0, HeaderLength);
                if (BodyLength != 0) Buffer.BlockCopy(packed, 12 + URILength + HeaderLength, Body, 0, BodyLength);
            }
        };
        //

        // Watson DeDupe
        LRUCache<string, byte[]> memoryCache;
        
        //Watson DeDupe
        DedupeLibrary deDupe;
        static List<Chunk> Chunks;
        //static string Key = "kjv";
        //static List<string> Keys;
        //static byte[] Data;
        const int deDupeMinChunkSize = 32768;
        const int deDupeMaxChunkSize = 262144;
        const int deDupeMaxMemoryCacheItems = 1000;

        static bool DebugDedupe = false;
        static bool DebugSql = false;
        static ulong NumObjects;
        static ulong NumChunks;
        static ulong LogicalBytes;
        static ulong PhysicalBytes;
        static decimal DedupeRatioX;
        static decimal DedupeRatioPercent;
        public static long maxObjectSizeHTTP;
        //END

        public TitaniumController()
        {


            proxyServer = new ProxyServer();

            // Setup Root certificates for machine
            proxyServer.CertificateManager.RootCertificateIssuerName = "NKLI DeDupe Engine";
            proxyServer.CertificateManager.RootCertificateName = "NKLI Certificate Authority";
            proxyServer.CertificateManager.SaveFakeCertificates = true;
            proxyServer.CertificateManager.EnsureRootCertificate();

            // Certificate trust is required to avoid host authentication errors!
            proxyServer.CertificateManager.TrustRootCertificate(true);
            proxyServer.CertificateManager.TrustRootCertificateAsAdmin(true);

            // Database storage
            if (!Directory.Exists("dbs")) Directory.CreateDirectory("dbs");
            // Required for certificate storage
            if (!Directory.Exists("crts")) Directory.CreateDirectory("crts");


            proxyServer.EnableHttp2 = true;

            // generate root certificate without storing it in file system
            //proxyServer.CertificateManager.CreateRootCertificate(false);


            // Fancy exception handling
            proxyServer.ExceptionFunc = async exception =>
            {
                if (exception is ProxyHttpException phex)
                {
                    await WriteToConsole(exception.Message + ": " + phex.InnerException?.Message + Environment.NewLine + exception.StackTrace, ConsoleColor.Red);
                    foreach (var pair in exception.Data)
                    {
                        await WriteToConsole(pair.ToString(), ConsoleColor.Red);
                    }
                }
                else
                {
                    await WriteToConsole(exception.Message + Environment.NewLine + exception.StackTrace, ConsoleColor.Red);
                    foreach (var pair in exception.Data)
                    {
                        await WriteToConsole(pair.ToString(), ConsoleColor.Red);
                    }
                }
            };

            proxyServer.TcpTimeWaitSeconds = 10;
            proxyServer.ConnectionTimeOutSeconds = 15;
            proxyServer.ReuseSocket = true;
            proxyServer.EnableConnectionPool = true;
            proxyServer.ForwardToUpstreamGateway = true;
            //proxyServer.ProxyBasicAuthenticateFunc = async (args, userName, password) =>
            //{
            //    return true;
            //};

            // this is just to show the functionality, provided implementations use junk value
            //proxyServer.GetCustomUpStreamProxyFunc = onGetCustomUpStreamProxyFunc;
            //proxyServer.CustomUpStreamProxyFailureFunc = onCustomUpStreamProxyFailureFunc;

            // optionally set the Certificate Engine
            // Under Mono or Non-Windows runtimes only BouncyCastle will be supported
            //proxyServer.CertificateManager.CertificateEngine = Network.CertificateEngine.BouncyCastle;

            // optionally set the Root Certificate
            //proxyServer.CertificateManager.RootCertificate = new X509Certificate2("myCert.pfx", string.Empty, X509KeyStorageFlags.Exportable);


            // Initialise chunk cache
            Console.WriteLine("<Titanium> Max disk cache, " + TitaniumHelper.FormatSize(chunkCacheMaxSize));
            if (!Directory.Exists(chunkCacheDir.Name)) Directory.CreateDirectory(chunkCacheDir.Name);
            chunkCache = new DiskCache<string>(chunkCacheDir, chunkCachePolicy, chunkCacheMaxSize, chunkCachePollingInterval);

            //Watson Cache: 1600 entries * 262144 max chunk size = Max 400Mb memory size
            Console.WriteLine("<Titanium> Max memory cache, " + TitaniumHelper.FormatSize(deDupeMaxChunkSize * deDupeMaxMemoryCacheItems));
            memoryCache = new LRUCache<string, byte[]>(deDupeMaxMemoryCacheItems, 100, false);
            //writeCache = new FIFOCache<string, byte[]>(1000, 100, false);

            //Watson DeDupe
            //if (!Directory.Exists("Chunks")) Directory.CreateDirectory("Chunks");
            long maxSizeMB = 500;
            maxObjectSizeHTTP = maxSizeMB * 1024 * 1024;
            Console.Write("<Titanium> Maximum supported HTTP object, " + TitaniumHelper.FormatSize(Int32.MaxValue) + " / Maximum cached HTTP object, " + TitaniumHelper.FormatSize(maxObjectSizeHTTP) + Environment.NewLine);


            if (File.Exists("dbs/dedupe.db"))
            {
                deDupe = new DedupeLibrary("dbs/dedupe.db", WriteChunk, ReadChunk, DeleteChunk, DebugDedupe, DebugSql);
            }
            else
            {
                deDupe = new DedupeLibrary("dbs/dedupe.db", deDupeMinChunkSize, deDupeMaxChunkSize, deDupeMinChunkSize, 2, WriteChunk, ReadChunk, DeleteChunk, DebugDedupe, DebugSql);
            }

            Console.Write(Environment.NewLine + "-------------------------" + Environment.NewLine + "DeDupe Engine Initialized" + Environment.NewLine + "-------------------------" + Environment.NewLine);
            
            // Gather index and dedupe stats
            if (deDupe.IndexStats(out NumObjects, out NumChunks, out LogicalBytes, out PhysicalBytes, out DedupeRatioX, out DedupeRatioPercent))
            {
                Console.WriteLine("  [Objects:" + NumObjects + "]/[Chunks:" + NumChunks + "] - [Logical:" + TitaniumHelper.FormatSize(LogicalBytes) + "]/[Physical:" + TitaniumHelper.FormatSize(PhysicalBytes) + "] + [Ratio:" + Math.Round(DedupeRatioPercent, 4) + "%]");
                //Console.WriteLine("  Dedupe ratio     : " + DedupeRatioX + "X, " + DedupeRatioPercent + "%");
                Console.WriteLine("-------------------------");
            }
            ///END Watson DeDupe
        }

        public void StartProxy(int listenPort, bool useSocksRelay, int socksProxyPort)
        {
            // THREAD - Disk write queue
            /*var threadWriteChunks = new Thread(async () =>
            {
                string decodeKey;
                while (true)
                {
                    while (writeCache.Count() != 0)
                    {
                        try
                        {
                            decodeKey = writeCache.Oldest();
                        }
                        catch
                        {
                            decodeKey = null;
                            await WriteToConsole("<Titanium> [ERROR] Expectedly ran out of keys in write cache", ConsoleColor.Red);
                        }

                        if (writeCache.TryGet(decodeKey, out byte[] Chunk))
                        {
                            // Then commit to disk
                            File.WriteAllBytes("Chunks\\" + decodeKey, Chunk);
                            using (var fs = new FileStream(
                                "Chunks\\" + decodeKey,
                                FileMode.Create,
                                FileAccess.Write,
                                FileShare.None,
                                0x1000,
                                FileOptions.WriteThrough))
                            {
                                fs.Write(Chunk, 0, Chunk.Length);
                                fs.Dispose();
                            }

                            writeCache.Remove(decodeKey);
                        }
                        Thread.Sleep(100);
                    }
                    Thread.Sleep(500);
                }
            } );*/

            // THREAD - Deduplication queue
            var threadDeDupe = new Thread(async () => {
                while (true)
                {
                    while (chunkLock > 0)
                    { Thread.Sleep(100); }
                    queueLock = true;

                    using (var session = chunkQueue.OpenSession())
                    {
                        var data = session.Dequeue();
                        session.Flush();

                        queueLock = false;
                        if (data == null) { Thread.Sleep(1000); continue; }

                        // First decode request from queue
                        try
                        {
                            ObjectStruct chunkStruct = new ObjectStruct();
                            chunkStruct.UnPackStruct(data);
                            
                            try
                            {
                                int bodyLength = chunkStruct.Body.Length;
                                string bodyURI = chunkStruct.URI;

                                string responseString = "";
                                if (bodyLength != 0)
                                {
                                    deDupe.StoreOrReplaceObject(chunkStruct.URI + "Body", chunkStruct.Body, out Chunks);
                                    responseString = ("<Titanium> (DeDupe) stored object, Size:" + TitaniumHelper.FormatSize(bodyLength) + ", Chunks:" + Chunks.Count + ", URI:" + bodyURI + Environment.NewLine);
                                }
                                else responseString = ("<Titanium> (DeDupe) refreshed object, URI:" + bodyURI + Environment.NewLine);
                                

                                deDupe.StoreOrReplaceObject(chunkStruct.URI + "Headers", chunkStruct.Headers, out Chunks);

                                if (deDupe.IndexStats(out NumObjects, out NumChunks, out LogicalBytes, out PhysicalBytes, out DedupeRatioX, out DedupeRatioPercent))
                                    await WriteToConsole(responseString + "                    [Objects:" + NumObjects + "]/[Chunks:" + NumChunks + "] - [Logical:" + TitaniumHelper.FormatSize(LogicalBytes) + "]/[Physical:" + TitaniumHelper.FormatSize(PhysicalBytes) + "] + [Ratio:" + Math.Round(DedupeRatioPercent, 4) + "%]", ConsoleColor.Yellow);
                            }
                            catch { await WriteToConsole("<Titanium> [ERROR] Dedupilication attempt failed, URI:" + chunkStruct.URI, ConsoleColor.Red); }

                            //session.Flush();
                        }
                        catch (Exception err)
                        {
                            await WriteToConsole("<Titanium> [ERROR] Exception occured while unpacking from deduplication queue." + err, ConsoleColor.Red);
                            continue;
                        }

                        
                    }
                }
            });

            // We prepare the DeDuplication queue before starting the worker threads
            if (File.Exists("chunkQueue//lock")) DeleteChildren("chunkQueue", true);
            chunkQueue = new PersistentQueue("chunkQueue");
            chunkQueue.Internals.ParanoidFlushing = true;

            //threadWriteChunks.Priority = ThreadPriority.BelowNormal;
            //threadWriteChunks.IsBackground = true;
            //threadWriteChunks.Start();

            threadDeDupe.Priority = ThreadPriority.Lowest;
            threadDeDupe.IsBackground = true;
            threadDeDupe.Start();

            RemotePort_Socks = socksProxyPort;

            proxyServer.BeforeRequest += OnRequest;
            proxyServer.BeforeResponse += OnResponse;
            proxyServer.AfterResponse += OnAfterResponse;

            proxyServer.ServerCertificateValidationCallback += OnCertificateValidation;
            proxyServer.ClientCertificateSelectionCallback += OnCertificateSelection;

            //proxyServer.EnableWinAuth = true;

            explicitEndPoint = new ExplicitProxyEndPoint(IPAddress.Any, listenPort);

            // Fired when a CONNECT request is received
            explicitEndPoint.BeforeTunnelConnectRequest += OnBeforeTunnelConnectRequest;
            explicitEndPoint.BeforeTunnelConnectResponse += OnBeforeTunnelConnectResponse;

            // An explicit endpoint is where the client knows about the existence of a proxy
            // So client sends request in a proxy friendly manner
            proxyServer.AddEndPoint(explicitEndPoint);
            proxyServer.Start();

            // Transparent endpoint is useful for reverse proxy (client is not aware of the existence of proxy)
            // A transparent endpoint usually requires a network router port forwarding HTTP(S) packets or DNS
            // to send data to this endPoint
            //var transparentEndPoint = new TransparentProxyEndPoint(IPAddress.Any, 443, true)
            //{ 
            //    // Generic Certificate hostname to use
            //    // When SNI is disabled by client
            //    GenericCertificateName = "google.com"
            //};

            //proxyServer.AddEndPoint(transparentEndPoint);
            //proxyServer.UpStreamHttpProxy = new ExternalProxy("localhost", 8888);
            //proxyServer.UpStreamHttpsProxy = new ExternalProxy("localhost", 8888);

            // SOCKS proxy
            if (useSocksRelay)
            {
                proxyServer.UpStreamHttpProxy = new ExternalProxy ("127.0.0.1", socksProxyPort)
                { ProxyType = ExternalProxyType.Socks5, UserName = "User1", Password = "Pass" };
                proxyServer.UpStreamHttpsProxy = new ExternalProxy("127.0.0.1", socksProxyPort)
                { ProxyType = ExternalProxyType.Socks5, UserName = "User1", Password = "Pass" };
            }

            // 
            /*proxyServer.SupportedSslProtocols =
                System.Security.Authentication.SslProtocols.Ssl2 |
                System.Security.Authentication.SslProtocols.Ssl3 |
                System.Security.Authentication.SslProtocols.Tls |
                System.Security.Authentication.SslProtocols.Tls11 |
                System.Security.Authentication.SslProtocols.Tls12 |
                System.Security.Authentication.SslProtocols.Tls13;*/

            // For speed
            proxyServer.CheckCertificateRevocation = System.Security.Cryptography.X509Certificates.X509RevocationMode.NoCheck;

            //var socksEndPoint = new SocksProxyEndPoint(IPAddress.Any, 1080, true)
            //{
            //    // Generic Certificate hostname to use
            //    // When SNI is disabled by client
            //    GenericCertificateName = "google.com"
            //};

            //proxyServer.AddEndPoint(socksEndPoint);

            foreach (var endPoint in proxyServer.ProxyEndPoints)
            {
                Console.WriteLine("Listening on '{0}' endpoint at Ip {1} and port: {2} ", endPoint.GetType().Name,
                    endPoint.IpAddress, endPoint.Port);
            }

            // Don't decrypt these domains
            dontDecrypt = new List<string> { "plex.direct", "activity.windows.com", "dropbox.com", "boxcryptor.com", "google.com" };

            // Only explicit proxies can be set as system proxy!
            //proxyServer.SetAsSystemHttpProxy(explicitEndPoint);
            //proxyServer.SetAsSystemHttpsProxy(explicitEndPoint);
            if (RunTime.IsWindows)
            {
                proxyServer.SetAsSystemProxy(explicitEndPoint, ProxyProtocolType.AllHttp);
            }
        }

        public void Stop()
        {
            explicitEndPoint.BeforeTunnelConnectRequest -= OnBeforeTunnelConnectRequest;
            explicitEndPoint.BeforeTunnelConnectResponse -= OnBeforeTunnelConnectResponse;

            proxyServer.BeforeRequest -= OnRequest;
            proxyServer.BeforeResponse -= OnResponse;
            proxyServer.ServerCertificateValidationCallback -= OnCertificateValidation;
            proxyServer.ClientCertificateSelectionCallback -= OnCertificateSelection;

            proxyServer.Stop();
            proxyServer.CertificateManager.Dispose();
            proxyServer.Dispose();

            chunkQueue.Dispose();
            //DeleteChildren("chunkQueue", true);

            // remove the generated certificates
            proxyServer.CertificateManager.RemoveTrustedRootCertificate();
        }

        private async Task<IExternalProxy> OnGetCustomUpStreamProxyFunc(SessionEventArgsBase arg)
        {
            arg.GetState().PipelineInfo.AppendLine(nameof(OnGetCustomUpStreamProxyFunc));

            // this is just to show the functionality, provided values are junk
            return new ExternalProxy
            {
                BypassLocalhost = true,
                HostName = "127.0.0.1",
                Port = RemotePort_Socks,
                ProxyType = ExternalProxyType.Socks5,
                //Password = "fake",
                //UserName = "fake",
                //UseDefaultCredentials = false
            };
        }

        private async Task<IExternalProxy> OnCustomUpStreamProxyFailureFunc(SessionEventArgsBase arg)
        {
            arg.GetState().PipelineInfo.AppendLine(nameof(OnCustomUpStreamProxyFailureFunc));

            // this is just to show the functionality, provided values are junk
            return new ExternalProxy
            {
                BypassLocalhost = true,
                HostName = "127.0.0.1",
                Port = RemotePort_Socks,
                ProxyType = ExternalProxyType.Socks5,
                //Password = "fake2",
                //UserName = "fake2",
                //UseDefaultCredentials = false
            };
        }

        private async Task OnBeforeTunnelConnectRequest(object sender, TunnelConnectSessionEventArgs e)
        {
            string hostname = e.HttpClient.Request.RequestUri.Host;
            e.GetState().PipelineInfo.AppendLine(nameof(OnBeforeTunnelConnectRequest) + ":" + hostname);
            await WriteToConsole("Tunnel to: " + hostname);

            var clientLocalIp = e.ClientLocalEndPoint.Address;
            if (!clientLocalIp.Equals(IPAddress.Loopback) && !clientLocalIp.Equals(IPAddress.IPv6Loopback))
            {
                e.HttpClient.UpStreamEndPoint = new IPEndPoint(clientLocalIp, 0);
            }

            foreach (string value in dontDecrypt)
            {
                if (hostname.Contains(value))
                {
                    // Exclude Https addresses you don't want to proxy
                    // Useful for clients that use certificate pinning
                    // for example dropbox.com
                    e.DecryptSsl = false;
                }
            }

        }

        private void WebSocket_DataSent(object sender, DataEventArgs e)
        {
            var args = (SessionEventArgs)sender;
            WebSocketDataSentReceived(args, e, true);
        }

        private void WebSocket_DataReceived(object sender, DataEventArgs e)
        {
            var args = (SessionEventArgs)sender;
            WebSocketDataSentReceived(args, e, false);
        }

        private void WebSocketDataSentReceived(SessionEventArgs args, DataEventArgs e, bool sent)
        {
            var color = sent ? ConsoleColor.Green : ConsoleColor.Blue;

            foreach (var frame in args.WebSocketDecoder.Decode(e.Buffer, e.Offset, e.Count))
            {
                if (frame.OpCode == WebsocketOpCode.Binary)
                {
                    var data = frame.Data.ToArray();
                    string str = string.Join(",", data.ToArray().Select(x => x.ToString("X2")));
                    //writeToConsole(str, color).Wait();
                }

                if (frame.OpCode == WebsocketOpCode.Text)
                {
                    //writeToConsole(frame.GetText(), color).Wait();
                }
            }
        }

        private Task OnBeforeTunnelConnectResponse(object sender, TunnelConnectSessionEventArgs e)
        {
            e.GetState().PipelineInfo.AppendLine(nameof(OnBeforeTunnelConnectResponse) + ":" + e.HttpClient.Request.RequestUri);

            return Task.CompletedTask;
        }

        // intercept & cancel redirect or update requests
        private async Task OnRequest(object sender, SessionEventArgs e)
        {
            e.GetState().PipelineInfo.AppendLine(nameof(OnRequest) + ":" + e.HttpClient.Request.RequestUri);

            var clientLocalIp = e.ClientLocalEndPoint.Address;
            if (!clientLocalIp.Equals(IPAddress.Loopback) && !clientLocalIp.Equals(IPAddress.IPv6Loopback))
            {
                e.HttpClient.UpStreamEndPoint = new IPEndPoint(clientLocalIp, 0);
            }

            /*if (e.HttpClient.Request.Url.Contains("yahoo.com"))
            {
                e.CustomUpStreamProxy = new ExternalProxy("localhost", 8888);
            }*/

            await WriteToConsole("Active Client Connections:" + ((ProxyServer)sender).ClientConnectionCount);
            await WriteToConsole(e.HttpClient.Request.Url);

            // store it in the UserData property
            // It can be a simple integer, Guid, or any type
            /*try
            {
                e.UserData = new CustomUserData()
                {
                    RequestBody = e.HttpClient.Request.HasBody ? e.HttpClient.Request.Body : null,
                    //RequestBodyString = e.HttpClient.Request.HasBody ? e.HttpClient.Request.BodyString : null,
                    RequestMethod = e.HttpClient.Request.Method
                };
            }
            catch { await writeToConsole("<Titanium> (onRequest) Error = new CustomUserData", ConsoleColor.Red); }*/



            ////This sample shows how to get the multipart form data headers
            //if (e.HttpClient.Request.Host == "mail.yahoo.com" && e.HttpClient.Request.IsMultipartFormData)
            //{
            //    e.MultipartRequestPartSent += MultipartRequestPartSent;
            //}


            // We only attempt to replay from cache if cached headers also exist.
            if (deDupe.ObjectExists(e.HttpClient.Request.Url + "Headers"))
            {
                chunkLock++;
                while (queueLock)
                { Thread.Sleep(10); }

                try
                {
                    bool deleteCache = false;
                    // We only reconstruct the stream after successful object retrieval
                    if (deDupe.RetrieveObject(e.HttpClient.Request.Url + "Headers", out byte[] headerData))
                    {
                        // Convert byte[] back into dictionary
                        MemoryStream mStream = new MemoryStream();
                        BinaryFormatter binFormatter = new BinaryFormatter();
                        mStream.Write(headerData, 0, headerData.Length);
                        mStream.Position = 0;
                        Dictionary<string, string> restoredHeader = binFormatter.Deserialize(mStream) as Dictionary<string, string>;

                        mStream.Dispose();

                        // Complain if dictionary is unexpectedly empty
                        if (restoredHeader.Count == 0)
                        {
                            await WriteToConsole("<Titanium> [ERROR] (onRequest) Cache deserialization resulted in 0 headers", ConsoleColor.Red);
                        }
                        else
                        {
                            // Convert dictionary into response format
                            Dictionary<string, HttpHeader> headerDictionary = new Dictionary<string, HttpHeader>();
                            foreach (var pair in restoredHeader)
                            {
                                //await writeToConsole("Key:" + pair.Key + " Value:" + pair.Value, ConsoleColor.Green);
                                //e.HttpClient.Response.Headers.AddHeader(new HttpHeader(pair.Key, pair.Value));
                                headerDictionary.Add(pair.Key, new HttpHeader(pair.Key, pair.Value));
                            }
                            try
                            {
                                // Check if resource has expired
                                HttpHeader cacheExpires = new HttpHeader("Expires", DateTime.Now.AddYears(1).ToLongDateString() + " 00:00:00 GMT");
                                try
                                {
                                    HttpHeader header = e.HttpClient.Response.Headers.GetFirstHeader("Expires");
                                    if (header != null) cacheExpires = header;
                                }
                                catch
                                {
                                    await WriteToConsole("<Titanium> (onResponse) Exception occured inspecting cache-control header", ConsoleColor.Red);
                                }

                                if (TitaniumHelper.IsExpired(Convert.ToDateTime(cacheExpires.Value)))
                                {
                                    // If object has expired, then delete cached headers and revalidate against server
                                    deDupe.DeleteObject(e.HttpClient.Request.Url + "Headers");
                                    if (!e.HttpClient.Response.Headers.HeaderExists("If-Modified-Since")) e.HttpClient.Request.Headers.AddHeader("If-Modified-Since", DateTime.Now.ToUniversalTime().ToString("ddd, dd MMM yyyy HH:mm:ss 'GMT'"));
                                    await WriteToConsole("<Titanium> object expired URI:" + e.HttpClient.Request.Url, ConsoleColor.Green);
                                }
                                else
                                {

                                    // Check matching body exists and retrive
                                    if (deDupe.ObjectExists(e.HttpClient.Request.Url + "Body"))
                                    {
                                        deDupe.RetrieveObject(e.HttpClient.Request.Url + "Body", out byte[] objectData);

                                        // Get cached content-length
                                        if (headerDictionary.TryGetValue("Content-Length", out HttpHeader contentLength))
                                        {
                                            // Make sure both header content length and retrieved body length match
                                            if (Convert.ToInt64(contentLength.Value) == objectData.LongLength)
                                            {

                                                await WriteToConsole("<Titanium> found object, Size: " + TitaniumHelper.FormatSize(objectData.Length) + ", URI:" + e.HttpClient.Request.Url, ConsoleColor.Cyan);

                                                // Cancel request and respond cached data
                                                try { e.Ok(objectData, headerDictionary, true); }
                                                catch { await WriteToConsole("<Titanium> [ERROR] (onRequest) Failure while attempting to send recontructed request", ConsoleColor.Red); }
                                            }
                                            else
                                            {
                                                await WriteToConsole("<Titanium> Content/Body length mismatch during cache retrieval", ConsoleColor.Red);
                                                await WriteToConsole("<Titanium> [Content: " + TitaniumHelper.FormatSize(Convert.ToInt64(contentLength.Value)) + "], [Content: " + TitaniumHelper.FormatSize(objectData.LongLength) + "], URI:" + e.HttpClient.Request.Url, ConsoleColor.Red);
                                                deleteCache = true;
                                            }
                                        }
                                        else
                                        {
                                            await WriteToConsole("<Titanium> Content-Length header not found during cache retrieval", ConsoleColor.Red);
                                            deleteCache = true;
                                        }
                                    }
                                }
                            }
                            catch { await WriteToConsole("<Titanium> [ERROR] (onRequest) Failure while attempting to restore cached object", ConsoleColor.Red); }
                        }
                    }
                    // In case of object retrieval failing, we wan't to remove it from the Index
                    else deleteCache = true;

                    if (deleteCache)
                    {
                        deDupe.DeleteObject(e.HttpClient.Request.Url + "Headers");
                        deDupe.DeleteObject(e.HttpClient.Request.Url + "Body");
                    }
                }
                catch { await WriteToConsole("<Titanium> [ERROR] (onRequest) Failure while attempting to restore cached headers", ConsoleColor.Red); }

                chunkLock = Math.Max(0, chunkLock - 1);
            }     


            // To cancel a request with a custom HTML content
            // Filter URL
            //if (e.HttpClient.Request.RequestUri.AbsoluteUri.Contains("yahoo.com"))
            //{ 
            //    e.Ok("<!DOCTYPE html>" +
            //          "<html><body><h1>" +
            //          "Website Blocked" +
            //          "</h1>" +
            //          "<p>Blocked by titanium web proxy.</p>" +
            //          "</body>" +
            //          "</html>");
            //} 

            ////Redirect example
            //if (e.HttpClient.Request.RequestUri.AbsoluteUri.Contains("wikipedia.org"))
            //{ 
            //   e.Redirect("https://www.paypal.com");
            //} 
        }

        // Modify response
        private async Task MultipartRequestPartSent(object sender, MultipartRequestPartSentEventArgs e)
        {
            e.GetState().PipelineInfo.AppendLine(nameof(MultipartRequestPartSent));

            var session = (SessionEventArgs)sender;
            await WriteToConsole("Multipart form data headers:");
            foreach (var header in e.Headers)
            {
                await WriteToConsole(header.ToString());
            }
        }

        private async Task OnResponse(object sender, SessionEventArgs e)
        {
            e.GetState().PipelineInfo.AppendLine(nameof(OnResponse));

            if (e.HttpClient.ConnectRequest?.TunnelType == TunnelType.Websocket)
            {
                e.DataSent += WebSocket_DataSent;
                e.DataReceived += WebSocket_DataReceived;
            }

            await WriteToConsole("Active Server Connections:" + ((ProxyServer)sender).ServerConnectionCount);

            //string ext = System.IO.Path.GetExtension(e.HttpClient.Request.RequestUri.AbsolutePath);

            // access user data set in request to do something with it
            //var userData = e.HttpClient.UserData as CustomUserData;

            //if (ext == ".gif" || ext == ".png" || ext == ".jpg")
            //{ 
            //    byte[] btBody = Encoding.UTF8.GetBytes("<!DOCTYPE html>" +
            //                                           "<html><body><h1>" +
            //                                           "Image is blocked" +
            //                                           "</h1>" +
            //                                           "<p>Blocked by Titanium</p>" +
            //                                           "</body>" +
            //                                           "</html>");

            //    var response = new OkResponse(btBody);
            //    response.HttpVersion = e.HttpClient.Request.HttpVersion;

            //    e.Respond(response);
            //    e.TerminateServerConnection();
            //} 

            //// print out process id of current session
            //Console.WriteLine($"PID: {e.HttpClient.ProcessId.Value}");


            //if (!e.ProxySession.Request.Host.Equals("medeczane.sgk.gov.tr")) return;
            //if (!e.HttpClient.Request.Host.Equals("www.gutenberg.org")) return;
            if ((e.HttpClient.Request.Method == "GET" || e.HttpClient.Request.Method == "POST") 
                && ((e.HttpClient.Response.StatusCode == (int)HttpStatusCode.OK) || (e.HttpClient.Response.StatusCode == (int)HttpStatusCode.NotModified)))
            {

                byte[] output = new byte[0];
                string Key = e.HttpClient.Request.Url;
                try
                {
                    
                    bool bodyValidated = false;
                    bool bodyRetrieved = false;
                    // If resource NotModified, then attempt to retrieve from cache
                    if ((e.HttpClient.Response.StatusCode == (int)HttpStatusCode.NotModified) && (deDupe.ObjectExists(Key + "Body")))
                    {
                        if (deDupe.RetrieveObject(Key + "Body", out byte[] objectData))
                        {
                            await WriteToConsole("<Titanium> Re-validated object, Size: " + TitaniumHelper.FormatSize(objectData.Length) + ", URI:" + Key, ConsoleColor.Cyan);

                            // Respond cached data
                            try
                            {
                                output = objectData;
                                e.SetResponseBody(objectData);
                                e.HttpClient.Response.StatusCode = (int)HttpStatusCode.OK;
                                bodyValidated = true;
                                bodyRetrieved = true;
                            }
                            catch { await WriteToConsole("<Titanium> [ERROR] (OnResponse) Failure while attempting to send recontructed request", ConsoleColor.Red); }
                        }
                        // If body retrieval failed, then re-request
                        //if (!bodyRetrieved) e.ReRequest = true;
                        //e.TerminateSession();
                    }
                    // If !NotModified than receive response body from server
                    else if (e.HttpClient.Response.StatusCode != (int)HttpStatusCode.NotModified && e.HttpClient.Response.HasBody)
                    {
                        if (deDupe.ObjectExists(Key + "Body")) deDupe.DeleteObject(Key + "Body");
                        if (deDupe.ObjectExists(Key + "Headers")) deDupe.DeleteObject(Key + "Headers");

                        output = await e.GetResponseBody();
                        if (output.LongLength != 0) bodyRetrieved = true;
                    }
                 
                    // If no headers or body received than don't even bother
                    if ((e.HttpClient.Response.Headers.Count() != 0) && bodyRetrieved)
                    {
                        // We have what we need, close server connection early.
                        e.TerminateServerConnection();

                        // Retrive Cache-Control header
                        CacheControlHeaderValue cacheControl = new CacheControlHeaderValue();
                        try
                        {
                            HttpHeader header = e.HttpClient.Response.Headers.GetFirstHeader("Cache-Control");
                            if (header != null) cacheControl = CacheControlHeaderValue.Parse(header.Value);
                        }
                        catch
                        {
                            await WriteToConsole("<Titanium> (onResponse) Exception occured inspecting cache-control header", ConsoleColor.Red);
                        }

                        // Respect cache control headers
                        bool canCache = true;
                        if (cacheControl.NoCache || cacheControl.NoStore || cacheControl.MaxAge.HasValue)
                        {
                            string readableMessage = "";
                            // No-Store
                            if (cacheControl.NoStore)
                            {
                                canCache = false;
                                readableMessage += "(No-Store) ";
                            }
                            // No-Cache
                            if (cacheControl.NoCache)
                            {
                                readableMessage += "(No-Cache) ";
                                if ((!cacheControl.MaxAge.HasValue) && (canCache)) e.HttpClient.Response.Headers.AddHeader("Expires", DateTime.Now.ToUniversalTime().ToString("ddd, dd MMM yyyy HH:mm:ss 'GMT'"));
                            }
                            // Max-Age
                            if (cacheControl.MaxAge.HasValue)
                            {
                                readableMessage += "(Max-Age) ";
                                if (!e.HttpClient.Response.Headers.HeaderExists("Expires")) // Nin TODO - Test to ensure expirey whichever comes first if both exist
                                    e.HttpClient.Response.Headers.AddHeader("Expires", DateTime.Now.Add(cacheControl.MaxAge.Value).ToUniversalTime().ToString("ddd, dd MMM yyyy HH:mm:ss 'GMT'"));
                            }

                            await WriteToConsole("<Titanium> Respecting " + readableMessage + "header for key:" + Key, ConsoleColor.DarkYellow);
                        }
                        if (!canCache)
                        {
                            if (deDupe.ObjectExists(Key + "Body")) deDupe.DeleteObject(Key + "Body");
                            if (deDupe.ObjectExists(Key + "Headers")) deDupe.DeleteObject(Key + "Headers");
                        }
                        else
                        {
                            //if (cacheControl. )
                            //{
                            //    await WriteToConsole("<Titanium> Adding expires header based on Cache-Control Max-Age for key: " + Key, ConsoleColor.DarkGreen);
                            //    CacheControlHeaderValue CacheControlHeaderValue.Parse(cacheControl.Value);
                            //}


                            try
                            {
                                // For now we only want to avoid caching range-requests and put a min/max size on incoming objects
                                if ((output.LongLength == e.HttpClient.Response.ContentLength) &&
                                    (e.HttpClient.Response.ContentLength > 512) &&
                                    (maxObjectSizeHTTP > e.HttpClient.Response.ContentLength))
                                {

                                    // Serialize headers
                                    Dictionary<string, string> headerDictionary = new Dictionary<string, string>();
                                    IEnumerator<HttpHeader> headerEnumerator = e.HttpClient.Response.Headers.GetEnumerator();
                                    while (headerEnumerator.MoveNext())
                                    {
                                        if (!headerDictionary.ContainsKey(headerEnumerator.Current.Name))
                                            headerDictionary.Add(headerEnumerator.Current.Name, headerEnumerator.Current.Value);
                                    }
                                    var binFormatter = new BinaryFormatter();
                                    var mStream = new MemoryStream();
                                    binFormatter.Serialize(mStream, headerDictionary);

                                    // Store response body in cache provided it's the expected length
                                    try
                                    {
                                        ObjectStruct responseStruct = new ObjectStruct();
                                        if (!bodyValidated) responseStruct.PackStruct(e.HttpClient.Request.Url, mStream.ToArray(), output);
                                        else responseStruct.PackStruct(e.HttpClient.Request.Url, mStream.ToArray(), new byte[0]);
                                        mStream.Dispose();

                                        IPersistentQueueSession sessionQueue = chunkQueue.OpenSession();
                                        sessionQueue.Enqueue(responseStruct.packed);
                                        sessionQueue.Flush();
                                    }
                                    catch { await WriteToConsole("<Titanium> [ERROR] Unable to write response body to cache", ConsoleColor.Red); }

                                }
                            }
                            catch
                            {
                                //throw new Exception("Unable to output headers to cache");
                                await WriteToConsole("<Titanium> [ERROR] Unable to output headers to cache", ConsoleColor.Red);
                            }


                        }
                    }
                }
                catch
                {
                    await WriteToConsole("<Titanium> [ERROR] (onResponse) Exception occured receiving response body", ConsoleColor.Red);
                    //throw new Exception("<Titanium> (onResponse) Exception occured receiving response body");
                }

                /*if (e.HttpClient.Response.ContentType != null && e.HttpClient.Response.ContentType.Trim().ToLower().Contains("text/html"))
                {
                    var bodyBytes = await e.GetResponseBody();
                    e.SetResponseBody(TitaniumHelper.ToByteArray("M"));

                    string body = await e.GetResponseBodyAsString();
                    e.SetResponseBodyString("M");


                }*/
            }
            else
            {
                string reasonString = "[Method:" + e.HttpClient.Request.Method + "], [Code:" + e.HttpClient.Response.StatusCode + "], ";
                await WriteToConsole("<Titanium> Skipping " + reasonString + "[HasBody:" + e.HttpClient.Response.HasBody + "], URL:" + e.HttpClient.Request.Url, ConsoleColor.Magenta);
            }
        }

        private async Task OnAfterResponse(object sender, SessionEventArgs e)
        {
            // This is the massive console spam!
            //await writeToConsole($"Pipelineinfo: {e.GetState().PipelineInfo}", ConsoleColor.Magenta);

            // access user data set in request to do something with it
            //var userData = e.HttpClient.UserData as CustomUserData;
        }

        /// <summary>
        ///     Allows overriding default certificate validation logic
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        public Task OnCertificateValidation(object sender, CertificateValidationEventArgs e)
        {
            e.GetState().PipelineInfo.AppendLine(nameof(OnCertificateValidation));

            if (e.SslPolicyErrors == SslPolicyErrors.None)
            {
                e.IsValid = true;
            }
            return Task.CompletedTask;
        }

        /// <summary>
        ///     Allows overriding default client certificate selection logic during mutual authentication
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        public Task OnCertificateSelection(object sender, CertificateSelectionEventArgs e)
        {
            e.GetState().PipelineInfo.AppendLine(nameof(OnCertificateSelection));

            // set e.clientCertificate to override

            return Task.CompletedTask;
        }

        private async Task WriteToConsole(string message, ConsoleColor? consoleColor = null)
        {
            await @lock.WaitAsync();

            if (consoleColor.HasValue)
            {
                ConsoleColor existing = Console.ForegroundColor;
                Console.ForegroundColor = consoleColor.Value;
                Console.WriteLine(message);
                Console.ForegroundColor = existing;
            }
            else
            {
                Console.WriteLine(message);
            }

            @lock.Release();
        }


        ///// <summary>
        ///// User data object as defined by user.
        ///// User data can be set to each SessionEventArgs.HttpClient.UserData property
        ///// </summary>
        public class CustomUserData
        {
            public byte[] RequestBody { get; set; }
            public byte[] ResponseBody { get; set; }
            //public string RequestBodyString { get; set; }
            public string RequestMethod { get; set; }
            public int ResponsePosition { get; set; }
        }

        //
        #region Watson DeDupe

        bool WriteChunk(Chunk data)
        {
            // First commit to memory cache
            try
            {
                memoryCache.AddReplace(data.Key, data.Value);
            }
            catch
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("<Titanium> ERROR writing to memory cache, Key:" + data.Key);
                Console.ForegroundColor = ConsoleColor.White;
            }

            // Next compress & write to disk cache
            Stream chunkStream = new MemoryStream(CompressionFactory.Lzf4.Compress(data.Value, 3));
            chunkCache.SetValue(data.Key, chunkStream);
            chunkStream.Dispose();

            // Then write to disk
            /*try
            {
                File.WriteAllBytes("Chunks\\" + data.Key, data.Value);
                using (var fs = new FileStream(
                    "Chunks\\" + data.Key,
                    FileMode.Create,
                    FileAccess.Write,
                    FileShare.None,
                    0x1000,
                    FileOptions.WriteThrough))
                {
                    fs.Write(data.Value, 0, data.Value.Length);
                    //fs.Dispose();
                }
            }
            catch
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("<Titanium> ERROR writing to disk cache, Key:" + data.Key);
                Console.ForegroundColor = ConsoleColor.White;
            }*/

            return true;
        }
        byte[] ReadChunk(string key)
        {
            // Is possible return from memory, otherwise fetch from disk cache
            if (!memoryCache.TryGet(key, out byte[] data))
            {
                if (chunkCache.ContainsKey(key))
                {
                    if (chunkCache.TryGetValue(key, out Stream chunkStream))
                    {
                        //Console.WriteLine("!------------------ Attempting to decompress chunk");
                        return CompressionFactory.Lzf4.Decompress(chunkStream);
                    }
                    else return null;
                }
                else return null;


                // Returning null in case of file not found tells the calling function the object retrieval has failed
                /*if (File.Exists("Chunks\\" + key))
                {
                    byte[] readData = File.ReadAllBytes("Chunks\\" + key);
                    // Copy back read chunks to memory cache
                    memoryCache.AddReplace(key, readData);
                    return readData;
                }
                else return null;*/
            }
            else
            {
                return data;
            }
        }

        bool DeleteChunk(string key)
        {
            try
            {
                // First delete from memory
                if (memoryCache.Contains(key)) memoryCache.Remove(key);

                // Then delete from disk
                if (chunkCache.ContainsKey(key)) chunkCache.TryRemove(key); // Add code to handle locked files gracefully later!
                return true;
            }
            catch (Exception)
            {

            }
            return false;
        }
        #endregion
        //END

        static bool DeleteChildren(string directory, bool recursive)
        {
            var deletedAll = true;

            //Recurse if needed
            if (recursive)
            {
                //Enumerate the child directories
                foreach (var child in Directory.GetDirectories(directory))
                {
                    //Assume there are no locked files and just delete the directory - happy path
                    if (!TryDeleteDirectory(child))
                    {
                        //Couldn't delete it so we have to delete the children individually
                        if (!DeleteChildren(child, true))
                            deletedAll = false;
                    };
                };
            };

            //Now delete the files in the current directory
            foreach (var file in Directory.GetFiles(directory))
            {
                if (!TryDeleteFile(file))
                    deletedAll = false;
            };

            return deletedAll;
        }

        static bool TryDeleteDirectory(string filePath)
        {
            try
            {
                Directory.Delete(filePath, true);
                return true;
            }
            catch (IOException)
            {
                return false;
            }
            catch (UnauthorizedAccessException)
            {
                return false;
            }; //May need to add more exceptions to catch - DO NOT use a wildcard catch (e.g. Exception)
        }

        static bool TryDeleteFile(string filePath)
        {
            try
            {
                File.Delete(filePath);
                return true;
            }
            catch (IOException)
            {
                return false;
            }
            catch (UnauthorizedAccessException)
            {
                return false;
            }; //May need to add more exceptions to catch - DO NOT use a wildcard catch (e.g. Exception)
        }

        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    // TODO: dispose managed state (managed objects).
                    proxyServer.Dispose();

                    chunkCache.Dispose();

                    @lock.Dispose();
                }

                // TODO: free unmanaged resources (unmanaged objects) and override a finalizer below.
                // TODO: set large fields to null.

                disposedValue = true;
            }
        }

        // TODO: override a finalizer only if Dispose(bool disposing) above has code to free unmanaged resources.
        // ~TitaniumController() {
        //   // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
        //   Dispose(false);
        // }

        // This code added to correctly implement the disposable pattern.
        public void Dispose()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(true);
            // TODO: uncomment the following line if the finalizer is overridden above.
            // GC.SuppressFinalize(this);
        }
        #endregion

    }

    // Extension helper classes
    public class TitaniumClientState
    {
        public StringBuilder PipelineInfo { get; } = new StringBuilder();
    }

    public static class ProxyEventArgsBaseExtensions
    {
        public static TitaniumClientState GetState(this ProxyEventArgsBase args)
        {
            if (args.ClientUserData == null)
            {
                args.ClientUserData = new TitaniumClientState();
            }

            return (TitaniumClientState)args.ClientUserData;
        }
    }

    public static class TitaniumHelper
    {
        public static byte[] ToByteArray(this string str)
        {
            return System.Text.Encoding.ASCII.GetBytes(str);
        }

        public static bool IsExpired(this DateTime specificDate)
        {
            return specificDate < DateTime.Now;
        }

        public static string FormatSize(long size)
        {
            if (size > 1073741824) return String.Format("{0:n2}", ((decimal)size / (decimal)1073741824)) + "GB";
            else if (size > 1048576) return String.Format("{0:n2}", ((decimal)size / (decimal)1048576)) + "MB";
            else if (size > 1024) return String.Format("{0:n2}", ((decimal)size / (decimal)1024)) + "KB";
            else return size + "B";
        }

        public static string FormatSize(ulong size)
        {
            if (size > 1073741824) return String.Format("{0:n2}", ((decimal)size / (decimal)1073741824)) + "GB";
            else if (size > 1048576) return String.Format("{0:n2}", ((decimal)size / (decimal)1048576)) + "MB";
            else if (size > 1024) return String.Format("{0:n2}", ((decimal)size / (decimal)1024)) + "KB";
            else return size + "B";
        }

        public static string FormatSize(int size)
        {
            if (size > 1073741824) return String.Format("{0:n2}", ((decimal)size / (decimal)1073741824)) + "GB";
            else if (size > 1048576) return String.Format("{0:n2}", ((decimal)size / (decimal)1048576)) + "MB";
            else if (size > 1024) return String.Format("{0:n2}", ((decimal)size / (decimal)1024)) + "KB";
            else return size + "B";
        }
    }

}
