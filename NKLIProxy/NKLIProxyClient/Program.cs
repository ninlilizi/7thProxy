/*
    Socks5 - A full-fledged high-performance socks5 proxy server written in C#. Plugin support included.
    Copyright (C) 2016 ThrDev

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

using System;
using System.Net;
// Titanium Proxy
using Titanium.Web.Proxy.Helpers;


namespace NKLISocksClient
{
    class Program
    {
        public static int ListenerPort_Socks = 1080;
        public static int ListenerPort_HTTP = 8000;

        public static bool useSocksRelay = false;
        public static int RemotePort_Socks = 1081;
        public static string RemoteAddress_Socks = "localhost";

        public static NKLI.DeDupeProxy.TitaniumController HTTPProxy = new NKLI.DeDupeProxy.TitaniumController();

        static void Main(string[] args)
        {
            AppDomain.CurrentDomain.ProcessExit += new EventHandler(Graceful_Shutdown);


            // Handle HTTP Proxy
            if (RunTime.IsWindows)
            {
                // fix console hang due to QuickEdit mode
                NKLI.DeDupeProxy.NativeMethods.DisableQuickEditMode();
            }

            // Start proxy controller
            HTTPProxy.StartProxy(ListenerPort_HTTP, useSocksRelay, ListenerPort_Socks);

            Console.WriteLine("Hit ENTER key to exit..");
            Console.WriteLine();
            Console.Read();

            HTTPProxy.Stop();
        }



        static void Graceful_Shutdown(object sender, EventArgs e)
        {
            HTTPProxy.Stop();
        }

    }
}

