using System;
using System.Net;
using System.Runtime.InteropServices;

namespace DuckDBGeoparquet.Services
{
    /// <summary>
    /// Auto-detects the Windows system proxy for a target URL using
    /// <see cref="WebRequest.GetSystemWebProxy"/> and WinHTTP as fallback
    /// (IE proxy list, WPAD, and PAC).
    /// </summary>
    public static class SystemProxyDetector
    {
        public static HttpProxySettings DetectForS3() =>
            Detect(HttpProxySettings.DefaultS3TargetUrl);

        public static HttpProxySettings Detect(string targetUrl)
        {
            if (string.IsNullOrWhiteSpace(targetUrl))
                targetUrl = HttpProxySettings.DefaultS3TargetUrl;

            if (!Uri.TryCreate(targetUrl, UriKind.Absolute, out var target))
                return new HttpProxySettings { Source = HttpProxySettings.SourceSystem };

            var fromWebRequest = TryGetSystemWebProxy(target);
            if (fromWebRequest != null && fromWebRequest.IsConfigured)
                return fromWebRequest;

            var fromWinHttp = TryGetWinHttpProxy(target);
            if (fromWinHttp != null && fromWinHttp.IsConfigured)
                return fromWinHttp;

            return fromWebRequest ?? new HttpProxySettings { Source = HttpProxySettings.SourceSystem };
        }

        private static HttpProxySettings TryGetSystemWebProxy(Uri target)
        {
            try
            {
                IWebProxy proxy = WebRequest.GetSystemWebProxy();
                if (proxy == null)
                    return null;

                try
                {
                    proxy.Credentials ??= CredentialCache.DefaultCredentials;
                }
                catch
                {
                    // Default credentials are optional; the proxy URL still matters.
                }

                return HttpProxySettingsBuilder.FromWebProxy(proxy, target);
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"GetSystemWebProxy failed: {ex.Message}");
                return null;
            }
        }

        private static HttpProxySettings TryGetWinHttpProxy(Uri target)
        {
            if (!RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                return null;

            try
            {
                var ieConfig = new WINHTTP_CURRENT_USER_IE_PROXY_CONFIG();
                if (WinHttpGetIEProxyConfigForCurrentUser(ref ieConfig))
                {
                    try
                    {
                        string namedProxy = PtrToString(ieConfig.lpszProxy);
                        if (!string.IsNullOrWhiteSpace(namedProxy))
                        {
                            return HttpProxySettingsBuilder.ParseWinHttpProxyList(
                                namedProxy, target.AbsoluteUri);
                        }

                        string pacUrl = PtrToString(ieConfig.lpszAutoConfigUrl);
                        if (!string.IsNullOrWhiteSpace(pacUrl) || ieConfig.fAutoDetect)
                        {
                            var fromPac = TryWinHttpGetProxyForUrl(target, pacUrl, ieConfig.fAutoDetect);
                            if (fromPac != null && fromPac.IsConfigured)
                                return fromPac;
                        }
                    }
                    finally
                    {
                        Free(ieConfig.lpszAutoConfigUrl);
                        Free(ieConfig.lpszProxy);
                        Free(ieConfig.lpszProxyBypass);
                    }
                }

                return TryWinHttpGetProxyForUrl(target, autoConfigUrl: null, autoDetect: true);
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"WinHTTP proxy detection failed: {ex.Message}");
                return null;
            }
        }

        private static HttpProxySettings TryWinHttpGetProxyForUrl(Uri target, string autoConfigUrl, bool autoDetect)
        {
            IntPtr session = IntPtr.Zero;
            var info = new WINHTTP_PROXY_INFO();
            try
            {
                session = WinHttpOpen("DuckDBGeoparquet", WINHTTP_ACCESS_TYPE_NO_PROXY, null, null, 0);
                if (session == IntPtr.Zero)
                    return null;

                var options = new WINHTTP_AUTOPROXY_OPTIONS
                {
                    dwFlags = 0,
                    dwAutoDetectFlags = 0,
                    lpszAutoConfigUrl = autoConfigUrl,
                    lpvReserved = IntPtr.Zero,
                    dwReserved = 0,
                    fAutoLogonIfChallenged = true
                };

                if (!string.IsNullOrWhiteSpace(autoConfigUrl))
                    options.dwFlags |= WINHTTP_AUTOPROXY_CONFIG_URL;

                if (autoDetect)
                {
                    options.dwFlags |= WINHTTP_AUTOPROXY_AUTO_DETECT;
                    options.dwAutoDetectFlags = WINHTTP_AUTO_DETECT_TYPE_DHCP | WINHTTP_AUTO_DETECT_TYPE_DNS_A;
                }

                if (options.dwFlags == 0)
                    return null;

                if (!WinHttpGetProxyForUrl(session, target.AbsoluteUri, ref options, out info))
                    return null;

                string proxyList = PtrToString(info.lpszProxy);
                return HttpProxySettingsBuilder.ParseWinHttpProxyList(proxyList, target.AbsoluteUri);
            }
            finally
            {
                Free(info.lpszProxy);
                Free(info.lpszProxyBypass);
                if (session != IntPtr.Zero)
                    WinHttpCloseHandle(session);
            }
        }

        private static string PtrToString(IntPtr ptr) =>
            ptr == IntPtr.Zero ? null : Marshal.PtrToStringUni(ptr);

        private static void Free(IntPtr ptr)
        {
            if (ptr != IntPtr.Zero)
                GlobalFree(ptr);
        }

        private const uint WINHTTP_ACCESS_TYPE_NO_PROXY = 1;
        private const uint WINHTTP_AUTOPROXY_AUTO_DETECT = 0x00000001;
        private const uint WINHTTP_AUTOPROXY_CONFIG_URL = 0x00000002;
        private const uint WINHTTP_AUTO_DETECT_TYPE_DHCP = 0x00000001;
        private const uint WINHTTP_AUTO_DETECT_TYPE_DNS_A = 0x00000002;

        [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Unicode)]
        private struct WINHTTP_CURRENT_USER_IE_PROXY_CONFIG
        {
            [MarshalAs(UnmanagedType.Bool)]
            public bool fAutoDetect;
            public IntPtr lpszAutoConfigUrl;
            public IntPtr lpszProxy;
            public IntPtr lpszProxyBypass;
        }

        [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Unicode)]
        private struct WINHTTP_AUTOPROXY_OPTIONS
        {
            public uint dwFlags;
            public uint dwAutoDetectFlags;
            [MarshalAs(UnmanagedType.LPWStr)]
            public string lpszAutoConfigUrl;
            public IntPtr lpvReserved;
            public uint dwReserved;
            [MarshalAs(UnmanagedType.Bool)]
            public bool fAutoLogonIfChallenged;
        }

        [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Unicode)]
        private struct WINHTTP_PROXY_INFO
        {
            public uint dwAccessType;
            public IntPtr lpszProxy;
            public IntPtr lpszProxyBypass;
        }

        [DllImport("winhttp.dll", SetLastError = true, CharSet = CharSet.Unicode)]
        private static extern bool WinHttpGetIEProxyConfigForCurrentUser(
            ref WINHTTP_CURRENT_USER_IE_PROXY_CONFIG pProxyConfig);

        [DllImport("winhttp.dll", SetLastError = true, CharSet = CharSet.Unicode)]
        private static extern IntPtr WinHttpOpen(
            string pszAgentW,
            uint dwAccessType,
            string pszProxyW,
            string pszProxyBypassW,
            uint dwFlags);

        [DllImport("winhttp.dll", SetLastError = true, CharSet = CharSet.Unicode)]
        private static extern bool WinHttpGetProxyForUrl(
            IntPtr hSession,
            string lpcwszUrl,
            ref WINHTTP_AUTOPROXY_OPTIONS pAutoProxyOptions,
            out WINHTTP_PROXY_INFO pProxyInfo);

        [DllImport("winhttp.dll", SetLastError = true)]
        private static extern bool WinHttpCloseHandle(IntPtr hInternet);

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern IntPtr GlobalFree(IntPtr hMem);
    }
}
