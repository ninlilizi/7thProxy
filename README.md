# About NKLIProxy
NKLIProxy is a Titanium based, caching, HTTP/HTTPS proxy server written in C#


# Features

All the functionality of Titanium, plus:

* Memory cache with LRU eviction policy
* DeDuplicated and compressed disk cache with selectable eviction policies (LFU default).
* DeDupe/Compression handled by low-priority worker thread to ensure least possible impact on interactivity.

# Usage

Unzip package and execute NKLIProxy.exe.  
Elevation is requried to automatically manage system certificates. System proxy settings shall be configured automatically.  
On first execution a CA cert shall be generated and user prompted to accept installation. This is required for caching HTTPS content. Failure to accept certificate installation will result in certificate errors on attempting to access any web-sites.  

To remove the installed certificate automatically. Press Return on the console window to initiate a clean shutdown. All other occasions, simply close the window to persist both the proxy cache and generated CA certificate across sessions.  

