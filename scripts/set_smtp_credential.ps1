[CmdletBinding(SupportsShouldProcess = $true)]
param(
    [string]$Target = "TempestWeatherSMTP",
    [string]$Username,
    [Security.SecureString]$Password,
    [ValidateSet("Session", "LocalMachine", "Enterprise")]
    [string]$Persist = "LocalMachine"
)

if (-not ("CredMan" -as [type])) {
    Add-Type -TypeDefinition @"
using System;
using System.Runtime.InteropServices;
public static class CredMan {
    public const int CRED_TYPE_GENERIC = 1;
    public const int CRED_PERSIST_SESSION = 1;
    public const int CRED_PERSIST_LOCAL_MACHINE = 2;
    public const int CRED_PERSIST_ENTERPRISE = 3;

    [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Unicode)]
    public struct CREDENTIAL {
        public UInt32 Flags;
        public UInt32 Type;
        public string TargetName;
        public string Comment;
        public System.Runtime.InteropServices.ComTypes.FILETIME LastWritten;
        public UInt32 CredentialBlobSize;
        public IntPtr CredentialBlob;
        public UInt32 Persist;
        public UInt32 AttributeCount;
        public IntPtr Attributes;
        public string TargetAlias;
        public string UserName;
    }

    [DllImport("Advapi32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
    public static extern bool CredWrite(ref CREDENTIAL credential, UInt32 flags);
}
"@
}

if (-not $Target) {
    throw "Target is required."
}
if (-not $Username) {
    $Username = Read-Host "SMTP username (Gmail address)"
}
if (-not $Password) {
    $Password = Read-Host "SMTP app password" -AsSecureString
}
if ([string]::IsNullOrWhiteSpace($Username)) {
    throw "Username is required."
}

$persistValue = switch ($Persist) {
    "Session" { [CredMan]::CRED_PERSIST_SESSION }
    "Enterprise" { [CredMan]::CRED_PERSIST_ENTERPRISE }
    default { [CredMan]::CRED_PERSIST_LOCAL_MACHINE }
}

$plain = $null
$bstr = [IntPtr]::Zero
$blobPtr = [IntPtr]::Zero
$bytes = $null

try {
    $bstr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($Password)
    $plain = [Runtime.InteropServices.Marshal]::PtrToStringUni($bstr)
    if ([string]::IsNullOrEmpty($plain)) {
        throw "Password is required."
    }

    $bytes = [System.Text.Encoding]::Unicode.GetBytes($plain)
    $blobPtr = [Runtime.InteropServices.Marshal]::AllocHGlobal($bytes.Length)
    [Runtime.InteropServices.Marshal]::Copy($bytes, 0, $blobPtr, $bytes.Length)

    $cred = New-Object CredMan+CREDENTIAL
    $cred.Flags = 0
    $cred.Type = [CredMan]::CRED_TYPE_GENERIC
    $cred.TargetName = $Target
    $cred.Comment = "TempestWeather SMTP"
    $cred.CredentialBlobSize = $bytes.Length
    $cred.CredentialBlob = $blobPtr
    $cred.Persist = $persistValue
    $cred.AttributeCount = 0
    $cred.Attributes = [IntPtr]::Zero
    $cred.TargetAlias = $null
    $cred.UserName = $Username

    if ($PSCmdlet.ShouldProcess($Target, "Write Windows Credential Manager entry")) {
        $ok = [CredMan]::CredWrite([ref]$cred, 0)
        if (-not $ok) {
            $err = [Runtime.InteropServices.Marshal]::GetLastWin32Error()
            throw "CredWrite failed with error $err."
        }
    }
} finally {
    if ($bytes) {
        [Array]::Clear($bytes, 0, $bytes.Length)
    }
    if ($blobPtr -ne [IntPtr]::Zero) {
        [Runtime.InteropServices.Marshal]::FreeHGlobal($blobPtr)
    }
    if ($bstr -ne [IntPtr]::Zero) {
        [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr)
    }
}

Write-Host "Stored Credential Manager entry '$Target' for '$Username' (Persist: $Persist)."
