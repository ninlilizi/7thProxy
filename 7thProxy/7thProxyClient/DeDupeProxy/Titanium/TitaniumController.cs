using Caching;
using DiskQueue;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Security;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Titanium.Web.Proxy;
using Titanium.Web.Proxy.EventArguments;
using Titanium.Web.Proxy.Exceptions;
using Titanium.Web.Proxy.Http;
using Titanium.Web.Proxy.Models;
using Titanium.Web.Proxy.StreamExtended.Network;
using WatsonDedupe;
using Noemax.Compression;
using SJP.DiskCache;
using System.Net.Http.Headers;
using MessagePack;
using Titanium.Web.Proxy.Helpers;

namespace NKLI.DeDupeProxy
{
    public class TitaniumController : IDisposable
    {
        private readonly SemaphoreSlim @lock = new SemaphoreSlim(1);
        private ProxyServer proxyServer;
        private ExplicitProxyEndPoint explicitEndPoint;

        public static int RemotePort_Socks;

        public static List<string> dontDecrypt;
        public static List<string> overrideNoStoreNoCache;

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
        public bool chunkLock = false;
        public bool queueLock = false;

        private readonly MessagePackSerializerOptions messagePackOptions = MessagePackSerializerOptions.Standard.WithCompression(MessagePackCompression.Lz4BlockArray);

        // PackObject must be public type to avoid runtime exceptions!
        [MessagePackObject]
        public struct JournalStruct
        {
            [Key(0)]
            public string URI;

            [Key(1)]
            public byte[] Headers;

            [Key(2)]
            public byte[] Body;
        };

        [MessagePackObject]
        public class HeaderPackObject
        {
            [Key(0)]
            public Dictionary<string, string> Headers;

            public HeaderPackObject()
            {
                Headers = new Dictionary<string, string>();
            }
        };
        //


        // Memory Buffers
        ulong indexChunkQueue = 0;
        FIFOCache<ulong, byte[]> memoryJournalQueue;
        LRUCache<string, byte[]> memoryCacheBody;
        LRUCache<string, byte[]> memoryCacheHeaders;

        //Watson DeDupe
        DedupeLibrary deDupe;
        static List<Chunk> Chunks;
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
        public const long minObjectBytesHTTP = 256;
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


            proxyServer.EnableHttp2 = false; // Disabled as limited support prevents required header modifications

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
            proxyServer.ReuseSocket = false; // Possibly causing Netflix stalls
            proxyServer.EnableConnectionPool = true;
            proxyServer.Enable100ContinueBehaviour = false; // Disabled as may caused problems on broken servers
            proxyServer.EnableTcpServerConnectionPrefetch = true;
            proxyServer.ForwardToUpstreamGateway = true;

            // Double default thread count for throughput
            if (proxyServer.ThreadPoolWorkerThread == Environment.ProcessorCount) proxyServer.ThreadPoolWorkerThread = (Environment.ProcessorCount * 4);

            //proxyServer.ProxyBasicAuthenticateFunc = async (args, userName, password) =>
            //{
            //    return true;
            //};

            // this is just to show the functionality, provided implementations use junk value
            //proxyServer.GetCustomUpStreamProxyFunc = onGetCustomUpStreamProxyFunc;
            //proxyServer.CustomUpStreamProxyFailureFunc = onCustomUpStreamProxyFailureFunc;

            // optionally set the Certificate Engine
            // Under Mono or Non-Windows runtimes only BouncyCastle will be supported
            proxyServer.CertificateManager.CertificateEngine = Titanium.Web.Proxy.Network.CertificateEngine.BouncyCastle;

            // optionally set the Root Certificate
            //proxyServer.CertificateManager.RootCertificate = new X509Certificate2("myCert.pfx", string.Empty, X509KeyStorageFlags.Exportable);


            // Initialise chunk cache
            Console.WriteLine("<Cache> Max disk cache, " + TitaniumHelper.FormatSize(chunkCacheMaxSize));
            if (!Directory.Exists(chunkCacheDir.Name)) Directory.CreateDirectory(chunkCacheDir.Name);
            chunkCache = new DiskCache<string>(chunkCacheDir, chunkCachePolicy, chunkCacheMaxSize, chunkCachePollingInterval);

            // Callback removes objects from DeDupe index if referenced chunk is removed from cache
            chunkCache.EntryRemoved += ChunkRemoved;

            //Watson Cache: 1600 entries * 262144 max chunk size = Max 400Mb memory size
            Console.WriteLine("<Cache> Max memory cache, " + TitaniumHelper.FormatSize(deDupeMaxChunkSize * deDupeMaxMemoryCacheItems));
            memoryJournalQueue = new FIFOCache<ulong, byte[]>(10000, 100, false);
            memoryCacheBody = new LRUCache<string, byte[]>(deDupeMaxMemoryCacheItems, 100, false);
            memoryCacheHeaders = new LRUCache<string, byte[]>(deDupeMaxMemoryCacheItems, 100, false);
            //writeCache = new FIFOCache<string, byte[]>(1000, 100, false);

            //Watson DeDupe
            //if (!Directory.Exists("Chunks")) Directory.CreateDirectory("Chunks");
            long maxSizeMB = 500;
            maxObjectSizeHTTP = maxSizeMB * 1024 * 1024;
            Console.Write("<Cache> Maximum supported HTTP object, " + TitaniumHelper.FormatSize(Int32.MaxValue) + " / Maximum cached HTTP object, " + TitaniumHelper.FormatSize(maxObjectSizeHTTP) + Environment.NewLine);


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
            if (deDupe.IndexStats(out NumObjects, out int cachedObjects, out NumChunks, out LogicalBytes, out PhysicalBytes, out DedupeRatioX, out DedupeRatioPercent))
            {
                Console.WriteLine("  [Objects:" + NumObjects + "]/[Chunks:" + NumChunks + "] [CachedKeys: " + cachedObjects + "] - [Logical:" + TitaniumHelper.FormatSize(LogicalBytes) + "]/[Physical:" + TitaniumHelper.FormatSize(PhysicalBytes) + "] + [Ratio:" + Math.Round(DedupeRatioPercent, 4) + "%]");
                //Console.WriteLine("  Dedupe ratio     : " + DedupeRatioX + "X, " + DedupeRatioPercent + "%");
                Console.WriteLine("-------------------------");
            }
            ///END Watson DeDupe
        }

        public void StartProxy(int listenPort, bool useSocksRelay, int socksProxyPort)
        {
            // THREAD - journal queueing thread
            var threadJournalQueue = new Thread(async () =>
            {
                while (true)
                {
                    // Wait till we have some work to do
                    while (memoryJournalQueue.Count() == 0)
                    { Thread.Sleep(100); }

                    // Opportunity for buffer to fill more before batching to journal
                    Thread.Sleep(2500);

                    // Lock
                    chunkLock = true;

                    // Wait till unlocked
                    while (queueLock)
                    { Thread.Sleep(10); }

                    // Open session
                    IPersistentQueueSession sessionQueue = chunkQueue.OpenSession();

                    while (memoryJournalQueue.Count() > 0)
                    {
                        // Get last in
                        ulong index = memoryJournalQueue.Oldest();

                        try { if (memoryJournalQueue.TryGet(index, out byte[] entry)) sessionQueue.Enqueue(entry); }
                        catch (Exception ex) { await WriteToConsole("<Cache> Exception occured while appending chunk queue journal, exception:" + Environment.NewLine + ex, ConsoleColor.Red); }
                        finally { memoryJournalQueue.Remove(index); }

                    }
                    // End session
                    sessionQueue.Flush();

                    // Unlock
                    chunkLock = false;
                }
            });

            // THREAD - Deduplication queue
            var threadDeDupe = new Thread(async () =>
            {
                while (true)
                {
                    // Wait till unlocked
                    while (chunkLock)
                    { Thread.Sleep(100); }

                    // Lock
                    queueLock = true;

                    try
                    {
                        using (var session = chunkQueue.OpenSession())
                        {
                            var data = session.Dequeue();
                            session.Flush();

                            queueLock = false;
                            if (data == null) { Thread.Sleep(1000); continue; }

                            // First decode request from queue
                            try
                            {
                                JournalStruct journalStruct = MessagePackSerializer.Deserialize<JournalStruct>(data, messagePackOptions);

                                try
                                {
                                    int bodyLength = journalStruct.Body.Length;
                                    string bodyURI = journalStruct.URI;

                                    string responseString = "";
                                    if (bodyLength != 0)
                                    {
                                        deDupe.StoreOrReplaceObject("Body_" + journalStruct.URI, journalStruct.Body, out Chunks, "Body_");
                                        responseString = ("<Cache> (DeDupe) stored object, Size:" + TitaniumHelper.FormatSize(bodyLength) + ", Chunks:" + Chunks.Count + ", URI:" + bodyURI + Environment.NewLine);
                                    }
                                    // If no body supplied then just update the headers and refresh the body access counters
                                    else
                                    {
                                        if (deDupe.RetrieveObjectMetadata("Body_" + journalStruct.URI, out ObjectMetadata md))
                                        {
                                            foreach (Chunk chunk in md.Chunks)
                                            {
                                                RefreshChunk(chunk.Key);
                                            }

                                            responseString = ("<Cache> (DeDupe) refreshed object, URI:" + bodyURI + Environment.NewLine);
                                        }
                                    }

                                    deDupe.StoreOrReplaceObject("Headers_" + journalStruct.URI, journalStruct.Headers, out Chunks, "Headers_");

                                    if (deDupe.IndexStats(out NumObjects, out int cachedObjects, out NumChunks, out LogicalBytes, out PhysicalBytes, out DedupeRatioX, out DedupeRatioPercent))
                                        await WriteToConsole(responseString + "                    [Objects:" + NumObjects + "]/[Chunks:" + NumChunks + "] [CachedKeys: " + cachedObjects + "] - [Logical:" + TitaniumHelper.FormatSize(LogicalBytes) + "]/[Physical:" + TitaniumHelper.FormatSize(PhysicalBytes) + "] + [Ratio:" + Math.Round(DedupeRatioPercent, 4) + "%]", ConsoleColor.Yellow);
                                }
                                catch (Exception ex)
                                {
                                    await WriteToConsole("<Cache> [ERROR] Dedupilication attempt failed, URI:" + journalStruct.URI + Environment.NewLine + ex, ConsoleColor.Red);
                                }

                            }
                            catch (Exception ex)
                            {
                                await WriteToConsole("<Cache> [ERROR] Exception occured while unpacking from deduplication queue." + Environment.NewLine + ex, ConsoleColor.Red);
                                continue;
                            }


                        }
                    }
                    catch (Exception ex)
                    {
                        await WriteToConsole("<Cache> [ERROR] Worker queue locked on attempted open" + Environment.NewLine + ex, ConsoleColor.Red);
                    }
                }
            });

            // We prepare the DeDuplication queue before starting the worker threads
            if (File.Exists("chunkQueue//lock")) DeleteChildren("chunkQueue", true);
            chunkQueue = new PersistentQueue("chunkQueue", DiskQueue.Implementation.Constants._32Megabytes, false);
            chunkQueue.Internals.TrimTransactionLogOnDispose = true;
            chunkQueue.Internals.ParanoidFlushing = true;

            threadJournalQueue.Priority = ThreadPriority.BelowNormal;
            threadJournalQueue.IsBackground = true;
            threadJournalQueue.Start();

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
                proxyServer.UpStreamHttpProxy = new ExternalProxy("127.0.0.1", socksProxyPort)
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
            proxyServer.SupportedSslProtocols = System.Security.Authentication.SslProtocols.None;


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
            dontDecrypt = new List<string> {
                ".local", "digidump", "digidump.local", "plex.direct", "activity.windows.com", "messenger.live.com", "login.live.com", "skype.com", "trouter.io", "dropbox.com", "boxcryptor.com", "google.com", "mudfish.net", "steam-chat.com", "glasswire.com", "chat.redditmedia.com", "adobe.com", "adobe.io", "adobecc.com", "creativecloud.com", "unity.com", "unity3d.com", "discord.com", "npmjs.org", "ovh.com",
                "oculus.com" };
            // Override Cache-Control policy headers for these domains
            overrideNoStoreNoCache = new List<string> { "nflxvideo.net" };

            // Only explicit proxies can be set as system proxy!
            //proxyServer.SetAsSystemHttpProxy(explicitEndPoint);
            //proxyServer.SetAsSystemHttpsProxy(explicitEndPoint);

            // Only set as system proxy if running in Release mode
#if !DEBUG
            if (RunTime.IsWindows)
            {
                //proxyServer.SetAsSystemProxy(explicitEndPoint, ProxyProtocolType.AllHttp);
            }
#endif

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

            if (e.HttpClient.Request.Url.Contains("?"))
            {
                await WriteToConsole("Client Connections:" + ((ProxyServer)sender).ClientConnectionCount + ", " + e.HttpClient.Request.Url.Substring(0, e.HttpClient.Request.Url.IndexOf("?")), ConsoleColor.DarkGray);
                //await WriteToConsole("Client Connections:" + ((ProxyServer)sender).ClientConnectionCount + ", " + e.HttpClient.Request.Url.Substring(e.HttpClient.Request.Url.IndexOf("?")), ConsoleColor.DarkGreen);
            }
            else await WriteToConsole("Client Connections:" + ((ProxyServer)sender).ClientConnectionCount + ", " + e.HttpClient.Request.Url, ConsoleColor.DarkGray);

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
            string key = e.HttpClient.Request.Url;

            // Special handling for netflix
            TitaniumHelper.NetflixKeyMorph(key, out key, out ulong start, out ulong end);

            // We only attempt to replay from cache if cached headers also exist.
            if (deDupe.ObjectExists("Headers_" + key))
            {
                try
                {
                    bool deleteCache = false;
                    // We only reconstruct the stream after successful object retrieval
                    if (RetrieveHeaders(key, out Dictionary<string, HttpHeader> headerDictionary))
                    {
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
                                await WriteToConsole("<Cache> (onResponse) Exception occured inspecting cache-control header", ConsoleColor.Red);
                            }

                            // Retrive Cache-Control header
                            CacheControlHeaderValue cacheControl = new CacheControlHeaderValue();
                            try
                            {
                                HttpHeader header = e.HttpClient.Request.Headers.GetFirstHeader("Cache-Control");
                                if (header != null) cacheControl = CacheControlHeaderValue.Parse(header.Value);
                            }
                            catch
                            {
                                await WriteToConsole("<Cache> (onResponse) Exception occured inspecting cache-control header", ConsoleColor.Red);
                            }
                            // Respect cache control headers
                            bool canCache = true;
                            string readableMessage = " by ";
                            if (cacheControl.NoCache || cacheControl.NoStore || cacheControl.MaxAge.HasValue)
                            {

                                // No-Cache
                                if (cacheControl.NoCache)
                                {
                                    canCache = false;
                                    readableMessage += "(No-Cache) request";
                                }
                                // Max-Age
                                if (cacheControl.MaxAge.HasValue)
                                {
                                    if (cacheControl.MaxAge.Value.TotalSeconds == 0) canCache = false;
                                    readableMessage += "(Max-Age=0) request";
                                }
                            }

                            // Selectively override No-Store directives
                            string hostname = e.HttpClient.Request.RequestUri.Host;
                            foreach (string value in overrideNoStoreNoCache)
                            {
                                if (hostname.Contains(value))
                                {

                                    canCache = true;
                                }
                            }

                            // If resource has expired or client sent a revalidation request
                            if (TitaniumHelper.IsExpired(Convert.ToDateTime(cacheExpires.Value), DateTime.Now) || e.HttpClient.Response.Headers.HeaderExists("If-Modified-Since") || !canCache)
                            {
                                if (readableMessage == " by ") readableMessage += "response header";
                                // If object has expired, then delete cached headers and revalidate against server
                                if (!e.HttpClient.Response.Headers.HeaderExists("If-Modified-Since")) e.HttpClient.Request.Headers.AddHeader("If-Modified-Since", DateTime.Now.ToUniversalTime().ToString("ddd, dd MMM yyyy HH:mm:ss 'GMT'"));
                                await WriteToConsole("<Cache> object expired" + readableMessage + ", URI:" + TitaniumHelper.FormatURI(key), ConsoleColor.Green);
                            }
                            // Otherwise attempt to replay from cache
                            else if (canCache)
                            {

                                // Check matching body exists and retrive
                                if (deDupe.ObjectExists("Body_" + key))
                                {
                                    deDupe.RetrieveObject("Body_" + key, out byte[] objectData);

                                    // Get cached content-length
                                    if (headerDictionary.TryGetValue("Content-Length", out HttpHeader contentLength))
                                    {
                                        // Make sure both header content length and retrieved body length match
                                        if (Convert.ToInt64(contentLength.Value) == objectData.LongLength)
                                        {

                                            await WriteToConsole("<Cache> found object, Size: " + TitaniumHelper.FormatSize(objectData.Length) + ", URI:" + TitaniumHelper.FormatURI(key), ConsoleColor.Cyan);

                                            // Cancel request and respond cached data
                                            try
                                            {
                                                e.Ok(objectData, headerDictionary, true);
                                                e.TerminateServerConnection();
                                            }
                                            catch { await WriteToConsole("<Cache> [ERROR] (onRequest) Failure while attempting to send recontructed request", ConsoleColor.Red); }
                                        }
                                        else
                                        {
                                            await WriteToConsole("<Cache> Content/Body length mismatch during cache retrieval", ConsoleColor.Red);
                                            await WriteToConsole("<Cache> [Content: " + TitaniumHelper.FormatSize(Convert.ToInt64(contentLength.Value)) + "], [Content: " + TitaniumHelper.FormatSize(objectData.LongLength) + "], URI:" + TitaniumHelper.FormatURI(key), ConsoleColor.Red);
                                            deleteCache = true;
                                        }
                                    }
                                    else
                                    {
                                        await WriteToConsole("<Cache> Content-Length header not found during cache retrieval", ConsoleColor.Red);
                                        deleteCache = true;
                                    }
                                }
                            }
                        }
                        catch { await WriteToConsole("<Cache> [ERROR] (onRequest) Failure while attempting to restore cached object", ConsoleColor.Red); }

                    }
                    // In case of object retrieval failing, we wan't to remove it from the Index
                    else deleteCache = true;

                    if (deleteCache)
                    {
                        deDupe.DeleteObject("Headers_" + key);
                        deDupe.DeleteObject("Body_" + key);
                    }
                }
                catch { await WriteToConsole("<Cache> [ERROR] (onRequest) Failure while attempting to restore cached headers", ConsoleColor.Red); }
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

            if (e.HttpClient.Request.Url.Contains("?")) await WriteToConsole("Server Connections:" + ((ProxyServer)sender).ServerConnectionCount + ", " + e.HttpClient.Request.Url.Substring(0, e.HttpClient.Request.Url.IndexOf("?")), ConsoleColor.DarkGray);
            else await WriteToConsole("Server Connections:" + ((ProxyServer)sender).ServerConnectionCount + ", " + e.HttpClient.Request.Url, ConsoleColor.DarkGray);

            //await WriteToConsole("Active Server Connections:" + ((ProxyServer)sender).ServerConnectionCount + ", " + e.HttpClient.Request.Url, ConsoleColor.DarkGray);

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
                && ((e.HttpClient.Response.StatusCode == (int)HttpStatusCode.OK) || (e.HttpClient.Response.StatusCode == (int)HttpStatusCode.NotModified))
                && (!e.HttpClient.Response.IsChunked) && (!e.HttpClient.Request.RequestUri.IsLoopback))
            {

                byte[] output = new byte[0];
                string key = e.HttpClient.Request.Url;
                try
                {
                    // Special handling for netflix
                    TitaniumHelper.NetflixKeyMorph(key, out key, out ulong start, out ulong end);


                    // If no headers or body received than don't even bother
                    if ((e.HttpClient.Response.Headers.Count() != 0))
                    {
                        // Retrive Cache-Control header
                        CacheControlHeaderValue cacheControl = new CacheControlHeaderValue();
                        try
                        {
                            HttpHeader header = e.HttpClient.Response.Headers.GetFirstHeader("Cache-Control");
                            if (header != null) cacheControl = CacheControlHeaderValue.Parse(header.Value);
                        }
                        catch
                        {
                            await WriteToConsole("<Cache> (onResponse) Exception occured inspecting cache-control header", ConsoleColor.Red);
                        }

                        bool bodyValidated = false;
                        bool bodyRetrieved = false;
                        // If resource NotModified, then attempt to retrieve from cache
                        if ((e.HttpClient.Response.StatusCode == (int)HttpStatusCode.NotModified) && (deDupe.ObjectExists("Body_" + key)))
                        {
                            // Attempt to retrieve body
                            if (deDupe.RetrieveObject("Body_" + key, out byte[] objectData))
                            {
                                // Attempt to retrieve headers
                                if (RetrieveHeaders(key, out Dictionary<string, HttpHeader> headerDictionary))
                                {
                                    // Attempt to retrieve Content-Length header
                                    if (headerDictionary.TryGetValue("Content-Length", out HttpHeader ContentLengthHeader))
                                        if (ContentLengthHeader != null)
                                        {
                                            // Ensure the object has not been truncated
                                            if (Convert.ToInt64(ContentLengthHeader.Value) != objectData.LongLength) // TODO - Discover why this happens at all
                                            {
                                                await WriteToConsole("<Cache> Retrieved Object size mismath on validation, Header:" + Convert.ToInt64(ContentLengthHeader.Value) + ", Retrieved:" + objectData.LongLength + ", key:" + TitaniumHelper.FormatURI(key), ConsoleColor.Red);

                                                deDupe.DeleteObject("Headers_" + key);
                                                deDupe.DeleteObject("Body_" + key);
                                            }
                                            else
                                            {
                                                await WriteToConsole("<Cache> Re-validated object, Size: " + TitaniumHelper.FormatSize(objectData.Length) + ", URI:" + TitaniumHelper.FormatURI(key), ConsoleColor.Cyan);

                                                // Respond cached data
                                                try
                                                {
                                                    output = objectData;
                                                    e.SetResponseBody(objectData);
                                                    e.HttpClient.Response.StatusCode = (int)HttpStatusCode.OK;
                                                    bodyValidated = true;
                                                    bodyRetrieved = true;
                                                }
                                                catch { await WriteToConsole("<Cache> [ERROR] (OnResponse) Failure while attempting to send recontructed request", ConsoleColor.Red); }

                                            }
                                        }
                                }
                            }
                            // If body retrieval failed, then re-request
                            if (!bodyRetrieved) e.ReRequest = true;
                        }
                        // If !NotModified than receive response body from server
                        else if (e.HttpClient.Response.StatusCode != (int)HttpStatusCode.NotModified && e.HttpClient.Response.HasBody)
                        {
                            if (deDupe.ObjectExists("Body_" + key)) deDupe.DeleteObject("Body_" + key);
                            if (deDupe.ObjectExists("Headers_" + key)) deDupe.DeleteObject("Headers_" + key);

                            output = await e.GetResponseBody();
                            if (output.LongLength != 0) bodyRetrieved = true;
                        }

                        // We have what we need, close server connection early.
                        e.TerminateServerConnection();


                        //
                        if (bodyRetrieved)
                        {
                            // Selectively override No-Store No-Cache directives
                            bool overrideCache_Control = false;
                            string hostname = e.HttpClient.Request.RequestUri.Host;
                            foreach (string value in overrideNoStoreNoCache)
                            {
                                if (hostname.Contains(value))
                                {
                                    overrideCache_Control = true;
                                    await WriteToConsole("<Cache> Cache-Control: [Policy: Override] for key:" + TitaniumHelper.FormatURI(key), ConsoleColor.DarkYellow);
                                }
                            }

                            // Respect cache control headers
                            bool canCache = true;
                            if ((cacheControl.NoCache || cacheControl.NoStore || cacheControl.MaxAge.HasValue) && !overrideCache_Control)
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
                                    if (canCache) e.HttpClient.Response.Headers.AddHeader("Expires", DateTime.Now.ToUniversalTime().ToString("ddd, dd MMM yyyy HH:mm:ss 'GMT'"));
                                    readableMessage += "(No-Cache) ";

                                    // Let's not cache anything with cache control
                                    //  headers for now due to integrity failures
                                    canCache = false;
                                }
                                // Max-Age
                                if (cacheControl.MaxAge.HasValue)
                                {
                                    readableMessage += "(Max-Age=" + String.Format("{0:n0}", cacheControl.MaxAge.Value.TotalSeconds) + ") ";

                                    if ((!cacheControl.NoCache) && (canCache))
                                    {
                                        // Get DateTime indicated by Max-Age
                                        string maxAgeDateTime = DateTime.Now.Add(cacheControl.MaxAge.Value).ToUniversalTime().ToString("ddd, dd MMM yyyy HH:mm:ss 'GMT'");

                                        if (e.HttpClient.Response.Headers.HeaderExists("Expires"))
                                        {
                                            bool policyMaxAge = false;

                                            // Get Expires header
                                            HttpHeader cacheExpires = new HttpHeader("Expires", DateTime.Now.AddYears(1).ToUniversalTime().ToString("ddd, dd MMM yyyy HH:mm:ss 'GMT'"));
                                            try
                                            {
                                                HttpHeader header = e.HttpClient.Response.Headers.GetFirstHeader("Expires");
                                                if (header != null)
                                                {
                                                    if (header.Value != "0") // TODO - Needs further debugging
                                                    {
                                                        readableMessage = "(Expires) " + readableMessage;
                                                        cacheExpires = header;

                                                        // If Max-Age arrives before existing Expires header
                                                        try
                                                        {
                                                            if (TitaniumHelper.IsExpired(Convert.ToDateTime(cacheExpires.Value), Convert.ToDateTime(maxAgeDateTime))) policyMaxAge = true;
                                                        }
                                                        catch (Exception ex)
                                                        {
                                                            readableMessage += "[Policy: ERROR] ";
                                                            await WriteToConsole("<Cache> [ERROR] (onResponse) Exception occured comparing Expires/Max-Age" + Environment.NewLine + "maxAgeDateTime: " + maxAgeDateTime + ", Expires: " + cacheExpires.Value + Environment.NewLine + ex, ConsoleColor.Red);
                                                        }
                                                    }
                                                    else policyMaxAge = true;
                                                }
                                            }
                                            catch
                                            {
                                                await WriteToConsole("<Cache> (onResponse) Exception occured inspecting cache-control header", ConsoleColor.Red);
                                            }

                                            // If Max-Age arrives before existing Expires header
                                            if (policyMaxAge)
                                            {
                                                // Replace Expires with Max-Age derived timestape
                                                e.HttpClient.Response.Headers.RemoveHeader("Expires");
                                                e.HttpClient.Response.Headers.AddHeader("Expires", maxAgeDateTime);
                                                readableMessage += "[Policy: Max-Age] ";
                                            }
                                            else readableMessage += "[Policy: Expires] ";

                                        }
                                        // If Expires doesn't exist then add from MaxAge timestamp
                                        else
                                        {
                                            e.HttpClient.Response.Headers.AddHeader("Expires", maxAgeDateTime);
                                            readableMessage += "[Policy: Max-Age] ";
                                        }
                                    }
                                    else
                                    {
                                        if (!canCache) readableMessage += "[Policy: NoStore] ";
                                        else readableMessage += "[Policy: NoCache] ";
                                    }
                                }

                                await WriteToConsole("<Cache> Cache-Control: " + readableMessage + "header for key:" + TitaniumHelper.FormatURI(key), ConsoleColor.DarkYellow);
                            }
                            if (!canCache)
                            {
                                if (deDupe.ObjectExists("Body_" + key)) deDupe.DeleteObject("Body_" + key);
                                if (deDupe.ObjectExists("Headers_" + key)) deDupe.DeleteObject("Headers_" + key);
                            }
                            else
                            {
                                try
                                {
                                    // For now we only want to avoid caching range-requests and put a min/max size on incoming objects
                                    if ((output.LongLength == e.HttpClient.Response.ContentLength) &&
                                        (e.HttpClient.Response.ContentLength > minObjectBytesHTTP) &&
                                        (maxObjectSizeHTTP > e.HttpClient.Response.ContentLength))
                                    {

                                        // Serialize headers
                                        HeaderPackObject headerDictionary = new HeaderPackObject();
                                        IEnumerator<HttpHeader> headerEnumerator = e.HttpClient.Response.Headers.GetEnumerator();
                                        while (headerEnumerator.MoveNext())
                                        {
                                            if (!headerDictionary.Headers.ContainsKey(headerEnumerator.Current.Name))
                                                headerDictionary.Headers.Add(headerEnumerator.Current.Name, headerEnumerator.Current.Value);
                                        }
                                        try
                                        {
                                            JournalStruct journalStruct = new JournalStruct()
                                            {
                                                URI = key,
                                                Headers = MessagePackSerializer.Serialize(headerDictionary)
                                            };

                                            if (!bodyValidated) journalStruct.Body = output;
                                            else journalStruct.Body = new byte[0];

                                            // Append to the journal queue
                                            memoryJournalQueue.AddReplace(indexChunkQueue, MessagePackSerializer.Serialize(journalStruct, messagePackOptions));

                                            // Append unique key index
                                            if (ulong.MaxValue == indexChunkQueue) indexChunkQueue = 0;
                                            indexChunkQueue++;
                                        }
                                        catch (Exception ex)
                                        {
                                            await WriteToConsole("<Cache> [ERROR] Unable to write response body to cache" + Environment.NewLine + ex, ConsoleColor.Red);
                                        }
                                    }
                                    else
                                    {
                                        // We only want one of these as Content-Size mismatch could be reason for appearing oversized
                                        if (e.HttpClient.Response.ContentLength < minObjectBytesHTTP) await WriteToConsole("<Cache> Skipping cache as under " + TitaniumHelper.FormatSize(minObjectBytesHTTP) + ", key: " + TitaniumHelper.FormatURI(key));
                                        else if (maxObjectSizeHTTP < e.HttpClient.Response.ContentLength) await WriteToConsole("<Cache> Skipping cache as larger than configured Max Object Size, key: " + TitaniumHelper.FormatURI(key), ConsoleColor.Magenta);
                                        else if (output.LongLength != e.HttpClient.Response.ContentLength) await WriteToConsole("<Cache> Skipping cache due to mismatch between Content-Size and received object, key: " + TitaniumHelper.FormatURI(key), ConsoleColor.Magenta);

                                    }
                                }
                                catch (Exception ex)
                                {
                                    //throw new Exception("Unable to output headers to cache");
                                    await WriteToConsole("<Cache> [ERROR] Unable to output headers to cache" + Environment.NewLine + ex, ConsoleColor.Red);
                                }

                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    await WriteToConsole("<Cache> [ERROR] (onResponse) Exception occured receiving response body" + Environment.NewLine + ex, ConsoleColor.Red);
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
                await WriteToConsole("<Cache> Skipping " + reasonString + "[HasBody:" + e.HttpClient.Response.HasBody + "]" + "[isChunked: " + e.HttpClient.Response.IsChunked + "], URL:" + TitaniumHelper.FormatURI(e.HttpClient.Request.Url), ConsoleColor.Magenta);
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

            // access user data set in request to do something with it
            //var userData = e.ClientUserData as CustomUserData;


            // Lets ignore some errors with 
            if (e.SslPolicyErrors == SslPolicyErrors.None || (e.Certificate.Issuer == "Replify-CA" && e.SslPolicyErrors == SslPolicyErrors.RemoteCertificateNameMismatch))
            {
                e.IsValid = true;
            }
            else
            {
                WriteToConsole("Not encrypting due to SSL validation failure:" + e.SslPolicyErrors.ToString() + ", URI:" + e.Session.HttpClient.Request.Host, ConsoleColor.DarkRed);
                dontDecrypt.Add(e.Session.HttpClient.Request.Host);
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

        // Callback to remove referenced objects after chunk eviction
        public async void ChunkRemoved(object sender, ICacheEntry<string> cacheEntry)
        {
            // Get referenced objects
            deDupe.ListObjectsWithChunk(cacheEntry.Key, out List<string> keys);

            if (DebugDedupe) Console.WriteLine("Chunk evicted from cache, key:" + cacheEntry.Key + ", Listing objects with chunk, " + keys.Count() + " found");

            // Attempts to delete objects from DeDupe database
            foreach (string key in keys)
            {
                if (deDupe.DeleteObject(key)) if (DebugDedupe) await WriteToConsole("<DeDupe> Eviction of chunk '" + cacheEntry.Key + "' triggered removal of referenced object, Key:" + TitaniumHelper.FormatURI(key), ConsoleColor.DarkMagenta);
                    else if (DebugDedupe) await WriteToConsole("<DeDupe> Eviction of chunk '" + cacheEntry.Key + "', Failed to remove referenced object from cache, Key:" + TitaniumHelper.FormatURI(key), ConsoleColor.Red);
            }
        }

        bool RetrieveHeaders(string key, out Dictionary<string, HttpHeader> headerDictionary)
        {
            headerDictionary = new Dictionary<string, HttpHeader>();

            // We only attempt to replay from cache if cached headers also exist.
            if (deDupe.ObjectExists("Headers_" + key))
            {
                try
                {
                    //bool deleteCache = false;
                    // We only reconstruct the stream after successful object retrieval
                    if (deDupe.RetrieveObject("Headers_" + key, out byte[] headerData))
                    {
                        HeaderPackObject restoredHeader = MessagePackSerializer.Deserialize<HeaderPackObject>(headerData);

                        // Complain if dictionary is unexpectedly empty
                        if (restoredHeader.Headers.Count == 0)
                        {
                            WriteToConsole("<Cache> [ERROR] (onRequest) Cache deserialization resulted in 0 headers", ConsoleColor.Red);
                        }
                        else
                        {
                            // Convert dictionary into response format
                            foreach (var pair in restoredHeader.Headers) { headerDictionary.Add(pair.Key, new HttpHeader(pair.Key, pair.Value)); }

                            return true;
                        }
                    }
                }
                catch { WriteToConsole("<Cache> Error occured retrieving headers", ConsoleColor.Red); }
            }
            return false;
        }



        ///// <summary>
        ///// User data object as defined by user.
        ///// User data can be set to each SessionEventArgs.HttpClient.UserData property
        ///// </summary>
        public class CustomUserData
        {
            public bool certificateValid;
        }

        //
        #region Watson DeDupe

        bool WriteChunk(Chunk data)
        {
            // First commit to memory cache
            try
            {
                if (data.Key.StartsWith("Headers_"))
                {
                    //Console.WriteLine("Chunk written to memory, Header, Key: " + data.Key);
                    memoryCacheHeaders.AddReplace(data.Key.Remove(0, 8), data.Value);
                }
                if (data.Key.StartsWith("Body_"))
                {
                    //Console.WriteLine("Chunk written to memory, Body, Key: " + data.Key);
                    memoryCacheBody.AddReplace(data.Key.Remove(0, 5), data.Value);
                }
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("<Titanium> ERROR writing chunk to memory cache, Key:" + data.Key + Environment.NewLine + ex, ConsoleColor.Red);
                Console.ForegroundColor = ConsoleColor.White;
            }

            // If chunk already exists then validate and replace if necesary
            if (chunkCache.ContainsKey(data.Key))
            {
                if (chunkCache.TryGetValue(data.Key, out Stream chunkStream))
                {
                    //Console.WriteLine("!------------------ Attempting to decompress chunk");
                    byte[] oldChunk = CompressionFactory.Lzf4.Decompress(chunkStream);

                    // Return if existing chunk is correct
                    if (TitaniumHelper.ByteArrayCompare(oldChunk, data.Value)) return true;
                    // Otherwise replace with new data

                    // Attempt to delete existing chunk
                    if (!chunkCache.TryRemove(data.Key)) return false;

                }
                // If Key exists and is unreadable
                else
                {
                    // Attempt to delete
                    if (!chunkCache.TryRemove(data.Key)) return false;
                }
            }

            // Now we're ready to write a fresh chunk to disk
            try
            {
                // Next compress & write to disk cache
                Stream chunkStream = new MemoryStream(CompressionFactory.Lzf4.Compress(data.Value, 3));
                if (chunkCache.TrySetValue(data.Key, chunkStream))
                {
                    chunkStream.Dispose();
                    return true;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("<Titanium> ERROR writing chunk to disk, Key:" + data.Key + Environment.NewLine + ex, ConsoleColor.Red);
            }
            // If writing to disk cache failed
            return false;
        }

        byte[] ReadChunk(string key)
        {
            bool memoryCacheReceived = false;
            byte[] data = new byte[0];

            if (key.StartsWith("Headers_"))
            {
                //if (key.StartsWith("Headers_")) Console.WriteLine("Chunk found in memory, Header, Key: " + key);
                if (memoryCacheHeaders.TryGet(key.Remove(0, 8), out data)) memoryCacheReceived = true;
            }
            if (key.StartsWith("Body_"))
            {
                //if (key.StartsWith("Body_")) Console.WriteLine("Chunk found from memory, Body, Key: " + key);
                if (memoryCacheBody.TryGet(key.Remove(0, 5), out data)) memoryCacheReceived = true;
            }

            // Is possible return from memory, otherwise fetch from disk cache
            if (!memoryCacheReceived)
            {
                if (chunkCache.ContainsKey(key))
                {
                    if (chunkCache.TryGetValue(key, out Stream chunkStream))
                    {
                        //Console.WriteLine("Chunk retrieved from disk, Key: " + key);

                        //Console.WriteLine("!------------------ Attempting to decompress chunk");
                        byte[] unpackedChunk = CompressionFactory.Lzf4.Decompress(chunkStream);

                        // Writeback to memory
                        if (key.StartsWith("Headers_"))
                        {
                            memoryCacheHeaders.AddReplace(key.Remove(0, 8), unpackedChunk);
                        }
                        if (key.StartsWith("Body_"))
                        {
                            memoryCacheBody.AddReplace(key.Remove(0, 5), unpackedChunk);
                        }

                        return unpackedChunk;
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
                //if (key.StartsWith("Headers_")) Console.WriteLine("Chunk retrieved from memory, Header, Key: " + key);
                //if (key.StartsWith("Body_")) Console.WriteLine("Chunk retrieved from memory, Body, Key: " + key);
                return data;
            }
        }

        // Update access counters
        bool RefreshChunk(string key)
        {
            try
            {
                // Refresh chunk access counter on disk
                chunkCache.RefreshValue(key);

                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
            return false;
        }

        bool DeleteChunk(string key)
        {
            try
            {
                // First delete from memory
                if (key.StartsWith("Headers_")) if (memoryCacheHeaders.Contains(key.Remove(0, 8))) memoryCacheHeaders.Remove(key.Remove(0, 8));
                    else if (key.StartsWith("Body_")) if (memoryCacheBody.Contains(key.Remove(0, 5))) memoryCacheBody.Remove(key.Remove(0, 5));


                // Then delete from disk
                if (chunkCache.ContainsKey(key)) chunkCache.TryRemove(key); // Add code to handle locked files gracefully later!
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
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

                    chunkCache.EntryRemoved -= ChunkRemoved;
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

        // specificDate is the comparison, targetDate is the one your testing to be before
        public static bool IsExpired(this DateTime specificDate, DateTime targetDate)
        {
            return specificDate < targetDate;
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

        public static string FormatURI(string key)
        {
            return key.Substring(0, Math.Min(92, key.Length));
        }

        #region UNSAFE byte[] comparison - Let's do it fast
        [DllImport("msvcrt.dll", CallingConvention = CallingConvention.Cdecl)]
        static extern int memcmp(byte[] b1, byte[] b2, long count);

        public static bool ByteArrayCompare(byte[] b1, byte[] b2)
        {
            // Validate buffers are the same length.
            // This also ensures that the count does not exceed the length of either buffer.  
            return b1.Length == b2.Length && memcmp(b1, b2, b1.Length) == 0;
        }
        #endregion

        // Special handling for netflix
        public static bool NetflixKeyMorph(string key, out string outKey, out ulong start, out ulong end)
        {
            // Initialise outputs
            start = 0;
            end = 0;

            if (key.Contains("oca.nflxvideo.net"))
            {
                //await WriteToConsole("NETFLIX Request, Special handling old key: " + Key, ConsoleColor.DarkGray);

                // Replicate protocol prefix
                string protocolPrefix = "";
                if (key.StartsWith("https://")) protocolPrefix = "https://";
                else protocolPrefix = "http://";

                // Remove everything before this as denotes global CDN
                // locations and not the actual content
                outKey = protocolPrefix + key.Substring(key.IndexOf("oca.nflxvideo.net"));

                // Get range request
                int offset = key.IndexOf("/range/") + 7;
                string rangeString = key.Substring(offset, key.IndexOf("?", offset) - offset);

                int divideIndex = rangeString.IndexOf("-");
                string rangeStart = rangeString.Substring(0, divideIndex);
                string rangeEnd = rangeString.Substring(divideIndex + 1);

                // Attempt to parse to ulong
                if (!UInt64.TryParse(rangeStart, out start)) return false;
                if (!UInt64.TryParse(rangeEnd, out end)) return false;

                // Final sanity check
                if (start > end) return false;

                // Dev debugging
                Console.WriteLine("");
                Console.WriteLine("NETFLIX Request, Special handling new key: " + key, ConsoleColor.DarkGray);
                Console.WriteLine("NETFLIX Range request [rangeStart" + start + "] [rangeEnd:" + end + "] [rangeLength:" + rangeEnd + "]");

                return true;
            }
            else
            {
                outKey = key;
                return false;
            }
        }
    }

}