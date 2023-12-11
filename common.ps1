# environmental declarations
$SubstrateUri = "https://substrate.office.com"
$LocalUri = "https://localhost:44348"
$FiddlerProxy = "http://127.0.0.1:8888"

# route SPHome requests through inactive slot to protect service from extensive parallel download
$KMUri = "https://sdfv2-kms.svc.ms"
$GraphUri = "https://graph.microsoft.com"

# Substrate API request headers
$SPDFTestHeader = "X-SPDF-TestFeedback";

# Substrate API response headers
$SubstrateRequestId = "request-id";
$SubstrateBETarget = "X-CalculatedBETarget";
$SubstrateFEServer = "X-FEServer";
$Date = "Date";

$contentType = "application/json; charset=UTF-8"
$headers = @{}
$headers["Accept"] = $contentType
$headers["Content-Type"] = $contentType
$headers["Sec-Fetch-Site"]="cross-site"
$headers["Sec-Fetch-Mode"]="cors"
$headers["Sec-Fetch-Dest"]="empty"
$headers["X-ODataQuery"]="true"

$emptySecureString = New-Object -TypeName SecureString

enum PipelineType {
    Dogfood = 1
    Staging
    Live
    Staging_Yukon
    Staging_WhoKnowsWhat
    Staging_Teams
    Staging_Taxonomy
    Staging_Definitions
    KBState_Backup1
    KBState_Backup2
    Development
    Staging_DefinitionsVNext
    Staging_Turing
    LiveSnapshot
    LinkingSnapshot
    YukonSnapshot
    WKWSnapshot
    Experimental_MSR
    Experimental_01
    Experimental_02
    Experimental_03
    Experimental_04
    Experimental_05
    Experimental_06
    Experimental_07
    Experimental_08
    Experimental_09
    Experimental_10
    Experimental_11
    Experimental_12
    Experimental_13
    Experimental_14
    Experimental_15
    Experimental_16
    Experimental_17
    Experimental_18
    Experimental_19
    Experimental_20
    Experimental_21
    Experimental_22
    Experimental_23
    Experimental_24
    Experimental_25
    Experimental_26
    Experimental_27
    Experimental_28
    Experimental_29
    Experimental_30
    Experimental_31
    Experimental_32
    Experimental_33
    Experimental_34
    Experimental_35
    Experimental_36
    Experimental_37
    Experimental_38
    Experimental_39
    Experimental_40
    Experimental_41
    Experimental_42
    Experimental_43
    Experimental_44
    Experimental_45
}

enum WTQVersionType {
  V1
  V2
  V3
  V9
  V10
  V11
}

enum DefinitionType {
    All
    Present
    Missing
}

# This stuff needs PowerShell 7 or later
if ($PSVersionTable.PSVersion.Major -lt 7)
{
    # Need to upgrade your PowerShell environment
    Write-Warning "PowerShell 7 or greater required."
    Write-Warning "If you don't have it, use `"winget install --name PowerShell --exact`" from the command line to get and install the current stable version."
    Write-Warning "Or see https://aka.ms/PSWindows for more"
    exit
}

# Test for ADAL installation
$AdalPath = Split-Path -parent $PSCommandPath | Join-Path -ChildPath "Microsoft.IdentityModel.Clients.ActiveDirectory.dll"
Write-Verbose "Looking up Microsoft.IdentityModel.Clients.ActiveDirectory.dll in parent path"
if ((Test-Path $AdalPath) -eq $false)
{
    Write-Warning "Missing Microsoft.IdentityModel.Clients.ActiveDirectory.dll in `"$PSCommandPath`". Exiting.`n "
    exit
}
else {
    Write-Verbose "Loading $AdalPath"
    $bytes = [System.IO.File]::ReadAllBytes($AdalPath)
    [System.Reflection.Assembly]::Load($bytes) | Out-Null
    Add-Type -Path $AdalPath
}

#region ################################################# Authentication commandlets ##################################################

function Get-UserToken ()
{
    <#
    .SYNOPSIS
     Returns SecureString containing Bearer token to be used in REST requests
    .DESCRIPTION
    This function relies on Microsoft.IdentityModel.Clients.ActiveDirectory.dll
    to obtain an authentication token from the local user's cache or by
    requesting a new token from Azure Active Directory.  The process of
    requesting a token from Azure AD may prompt the user for credentials.
    The function will fail if there it is not possible to obtain a time-valid
    token that matches the UPN passed and the service
    .EXAMPLE
     Get-UserToken -upn:nikita@example.com -serviceuri:https://microsoft.sharepoint.com

     This command prompts for auth and retrieves a token that can be used
     at https://microsoft.sharepoint.com for nikita@example.com
    .OUTPUTS
    SecureString
    #>

    [CmdletBinding()]
    param(
        # User Principal Name of identity. Defaults to current user's UPN.
        [Parameter(Mandatory=$false)][string]$Upn = "",
        # URI to scope. Default is https://substrate.office.com
        [Parameter(Mandatory=$false)][string]$ServiceUri = $SubstrateUri,
        # Tenant Object ID. Default us "Common"
        [Parameter(Mandatory=$false)][string]$tenant = "common",
        # Whether to skip lookup of token cache
        [Parameter(Mandatory=$false)][switch]$SkipTokenCache,
        # Whether to copy resulting JSON Web Token in clear text to the Windows Clipboard
        [Parameter(Mandatory=$false)][switch]$CopyToClipboard,
        # Optional token to specify.  Default is token obtained for current authentication context.
        [Parameter(Mandatory=$false)][SecureString]$TokenToRenew,
        # Amount of time to use to reuse or before renewing token, 20 minutes is default
        [Parameter(Mandatory=$false)][Int]$minimumTimeAllowed = 1200,
        # Client ID for which to request the token
        [Parameter(Mandatory=$false)][string]$clientId  = "d3590ed6-52b3-4102-aeff-aad2292ab01c"  # ID for Microsoft Office
    )

    if ($TokenToRenew.Length -gt 0){
        # Get attributes of the token to renew for seeding, and skip using the one on hand

        $parsedToken = Parse-JWTtoken $TokenToRenew
        $Upn = $null -ne $parsedToken.Upn ? $parsedToken.Upn : $parsedToken.smtp  #Token UPN - fall back to SMTP attribute if UPN isn't present
        $ServiceUri = $parsedToken.aud  # Token audience
        $tenant = $parsedToken.tid   # Token tenant; would have been refined from default "common"

        # Grab UNIX date
        $expiryTime = (Get-Date -Date "1970-01-01 00:00:00Z").ToUniversalTime()
        $expiryTime = $expiryTime.AddSeconds($parsedToken.exp) # Token expiry time, conveniently in ticks since the dawn of UNIX time

        # Check to see that there's enough time left in token to use token passed
        $secondsBeforeExpiry = [Math]::Floor((New-TimeSpan -Start:(Get-Date).ToUniversalTime() -End:$expiryTime).TotalSeconds)
        if ($secondsBeforeExpiry -lt $minimumTimeAllowed)
        {
            # Time's almost up
            Write-Verbose "Renewing token for user `"$($parsedToken.Name)`", $Upn, URI $ServiceUri, tenant $tenant, expiring at $expiryTime UTC. (It's $([DateTime]::UtcNow) UTC and there are only $secondsBeforeExpiry s left.)"
            $needToRenew = $true
        }
        else {
            # No need to renew - token lasts another minimumTimeAllowed seconds
            Write-Verbose "Not renewing token for user `"$($parsedToken.Name)`", $Upn, URI $ServiceUri, tenant $tenant, expiring at $expiryTime UTC. (It's $([DateTime]::UtcNow) UTC and there are still $secondsBeforeExpiry s left.)"
            $needToRenew = $false
        }
    }

    # Fault in current logged in user UPN if needed (i.e. not specified in param or passed token)
    if ("" -eq $Upn) {
        # If UPN isn't passed, a shortcut to get current user UPN without off-box calls
        $Upn = Invoke-Expression 'whoami /upn'
        if ($null -eq $Upn){
            Write-Error "No UPN passed as parameter or available from current user." -ErrorAction:Stop
        }
        Write-Verbose "No user specified, using UPN for current user $Upn"
    }

    # get constants outta the way
    $authority = "https://login.microsoftonline.com/common"
    $redirectUri = "urn:ietf:wg:oauth:2.0:oob" # No redirect needed; result returned Out Of Band

    $headerValue = $null # This is what the function ends up returning

    $authContext = [Microsoft.IdentityModel.Clients.ActiveDirectory.AuthenticationContext]::new($authority, $false)

    Write-Verbose "Checking token cache for match"
    $tokens=$authContext.TokenCache.ReadItems()
    foreach ($t in $tokens){
        $currentTime = [DateTime]::UtcNow

        # Is there a valid token?
        if (($t.Resource -eq $ServiceUri) -and ($t.DisplayableId -eq $Upn) -and ($t.ExpiresOn.UtcDateTime -gt $currentTime)){
            # Got a hit - now check caller is asking to ignore or renew token
            if (($needToRenew) -or ($SkipTokenCache)) {
                # Need to remove this item from cache to force a new request
                Write-Verbose "Cached token count: $($($authContext.TokenCache.ReadItems()).Count). Clearing token for $Upn, URI $ServiceUri, expiring at $($t.ExpiresOn.UtcDateTime) UTC to force renewal."
                [void]$authContext.TokenCache.DeleteItem($t);
                Write-Verbose "Cached token count: $($($authContext.TokenCache.ReadItems()).Count)"
            }
            else {
                $headerValue = $t.AccessToken
                Write-Verbose "Cache hit: Found valid token for $Upn, URI $ServiceUri, expiring at $($t.ExpiresOn.UtcDateTime) UTC. (It's $currentTime UTC.)"
                break
            }
        }
        else {
            Write-Verbose "Cache miss: Not reusing token $($t.DisplayableId), URI $($t.Resource), expiring at $($t.ExpiresOn.UtcDateTime) UTC. (It's $currentTime UTC.)"
        }
    }

    if ($null -eq $headerValue) {
        # If a token that meets the requirements is already cached then the user will not be prompted.
        $promptBehaviour = [Microsoft.IdentityModel.Clients.ActiveDirectory.PlatformParameters]::new(0)  # Auto = 0 - Acquire token will prompt the user for credentials when necessary

        # RequiredDisplayableId = 2 - When a UserIdentifier of this type is passed in a token acquisition operation, the operation
        #                             is guaranteed to return a token issued for the user with corresponding UserIdentifier.DisplayableId (UPN)
        $username = [Microsoft.IdentityModel.Clients.ActiveDirectory.UserIdentifier]::new($Upn, 2)
        Write-Verbose "Acquiring token for $Upn, URI $ServiceUri"

        Write-Progress -Id 99 -Activity "Getting auth token for $Upn for scope $ServiceUri." -Status "Check for prompt."

        [Microsoft.IdentityModel.Clients.ActiveDirectory.AuthenticationResult]$authResult = $authContext.AcquireTokenAsync($ServiceUri, $clientId, $redirectUri, $promptBehaviour, $username).Result
        Write-Progress -Id 99 -Activity "Getting auth token for $Upn for scope $ServiceUri. Check for prompt." -Completed

        # Due to magic of reflection and async behaviour, $authResult is always null, so need to check cache for token match 
        Write-Verbose "Checking token cache for match"
        $tokens=$authContext.TokenCache.ReadItems()

        foreach ($t in $tokens){
            $currentTime = [DateTime]::UtcNow

            # Is there a valid token that matches input?
            if (($t.Resource -eq $ServiceUri) -and ($t.DisplayableId -eq $Upn) -and ($t.ExpiresOn.UtcDateTime -gt $currentTime)){
                $headerValue = $t.AccessToken
                Write-Verbose "Got token for $Upn, URI $ServiceUri, expiring at $($t.ExpiresOn.UtcDateTime) UTC. (It's $currentTime UTC.)"
            }
            else {
                Write-Verbose "Skipping token $($t.DisplayableId), URI $($t.Resource), expiring at $($t.ExpiresOn.UtcDateTime) UTC. (It's $currentTime UTC.)"
            }
        }
        if ($null -eq $headerValue){
            Write-Error "No token present for user value $Upn" -ErrorAction:Stop
        }
    }

    if($CopyToClipboard)
    {
        Write-Host "Authorization header value has been copied to clipboard`n"
        Set-Clipboard -Value $headerValue
    }

    # Make sure to encode as SecureString on way out
    return $headerValue | ConvertTo-SecureString -AsPlainText
}

function ValidateAndSetToken ()
{
    param (
        [Parameter(Mandatory=$true)][SecureString]$token,
        [Parameter(Mandatory=$false)][String]$tokenUri
    )

    if ($token.Length -gt 0) {

        try {
            # Expired token - renew if possible
            $token = Get-UserToken -TokenToRenew:$token
        } catch {
            $token = Get-UserToken -service:$tokenUri    
        }
    }
    else {
        # Get default token to make call to Office Substrate Knowledge Base API
        $token = Get-UserToken -service:$tokenUri
    }

    return $token
}

function Parse-JWTtoken ()
{
    <#
    .SYNOPSIS
    Function to return headers and values set in JWT / Bearer token
    .DESCRIPTION
    Source: https://www.michev.info/Blog/Post/2140/decode-jwt-access-and-id-tokens-via-powershell
    Author: Vasil Michev, vasil at michev.info
    .OUTPUTS
    PSObject containing array of token headers and corresponding values
    #>

    [cmdletbinding()]
    param(
        # SecureString containing a JSON Web Token. See https://jwt.io/ for an interactive version.
        [Parameter(Mandatory=$true,ValueFromPipeline=$true)][SecureString]$SecureToken)

    if ($SecureToken.Length -gt 0)
    {
        $token = ConvertFrom-SecureString $SecureToken -AsPlainText
    }
    else {
        Write-Error "Zero-length token specified" -ErrorAction:Stop
    }

    Write-Verbose "Beginning parse of token"
    #Validate as per https://tools.ietf.org/html/rfc7519
    #Access and ID tokens are fine, Refresh tokens will not work
    if (!$token.Contains(".") -or !$token.StartsWith("eyJ")) { Write-Error "Invalid token" -ErrorAction Stop }

    #Header
    $tokenheader = $token.Split(".")[0].Replace('-', '+').Replace('_', '/')
    #Fix padding as needed, keep adding "=" until string length modulus 4 reaches 0
    while ($tokenheader.Length % 4) { Write-Verbose "Invalid length for a Base-64 char array or string, adding ="; $tokenheader += "=" }
    Write-Verbose "Base64 encoded (padded) header:"
    Write-Verbose $tokenheader
    #Convert from Base64 encoded string to PSObject all at once
    # Write-Verbose "Decoded header:" + ([System.Text.Encoding]::ASCII.GetString([system.convert]::FromBase64String($tokenheader)) | ConvertFrom-Json | fl | Out-Default)

    #Payload
    $tokenPayload = $token.Split(".")[1].Replace('-', '+').Replace('_', '/')
    #Fix padding as needed, keep adding "=" until string length modulus 4 reaches 0
    while ($tokenPayload.Length % 4) { Write-Verbose "Invalid length for a Base-64 char array or string, adding ="; $tokenPayload += "=" }
    Write-Verbose "Base64 encoded (padded) payload:"
    Write-Verbose $tokenPayload
    #Convert to Byte array
    $tokenByteArray = [System.Convert]::FromBase64String($tokenPayload)
    #Convert to string array
    $tokenArray = [System.Text.Encoding]::ASCII.GetString($tokenByteArray)
    Write-Verbose "Decoded array in JSON format:"
    Write-Verbose $tokenArray
    #Convert from JSON to PSObject
    $tokobj = $tokenArray | ConvertFrom-Json

    return $tokobj
}

function Test-TokenValidity ()
{    <#
    .SYNOPSIS
    Function to validate whether token passed is valid for time and audience
    .DESCRIPTION
    Tests extracted expiry time of token against machine clock time and audience
    and returns True if token is time-valid and audience-valid.
    .OUTPUTS
    Boolean indicating time-validity of token
    #>

    [CmdletBinding()]
    param (
        # Token to test for time-validity
        [Parameter(Mandatory=$true)][SecureString]$token,
        [Parameter(Mandatory=$false)][SecureString]$audience = $null
    )

    $parsedToken = Parse-JWTtoken($token)
    $expiryTime = (Get-Date -Date "1970-01-01 00:00:00Z").ToUniversalTime()
    $expiryTime = $expiryTime.AddSeconds($parsedToken.exp)
    $currentTime = [DateTime]::UtcNow

    Write-Verbose "Testing validity of token for user `"$($parsedToken.Name)`", $($parsedToken.Upn), URI $($parsedToken.Aud), tenant $($parsedToken.tid)."

    if (($null -ne $audience) -and ($parsedToken.Aud -ne $audience)) {
        Write-Verbose "Caller asserts token should be valid for $audience"
        return $false
    }

    Write-Verbose "Token expires at $expiryTime UTC. (It's $currentTime UTC.)"
    if ($expiryTime -gt $currentTime) {
        $validfor=[Math]::Floor((New-Timespan -start:$currentTime -end:$expiryTime).TotalSeconds)
        Write-Verbose "Token is valid for $validfor seconds."
        return $true
    }
    else {
        return $false
    }
}
# endregion

#endregion
#region ################################################## Utility Code ##################################################

function AddSubstrateRouteHeaders ([SecureString]$token)
{
    $parsedToken = Parse-JWTtoken($token)

    # Required for correct routing of calls to Exchange Online
    $headers["X-AnchorMailbox"] = "SMTP:" + $parsedToken.upn
    $headers["X-RoutingParameter-SessionKey"] = "SMTP:" + $parsedToken.upn
}

function Convert-HtmlToText ()
{
     <#
    .SYNOPSIS
     Converts HTML text to a plain text string, using Word!
    .DESCRIPTION
     Super-heavy but reliable conversion of HTML to text by invoking Word to load source
     text stream, then export as Unicode text
    .EXAMPLE
     Convert-HtmlToText "<!DOCTYPE html><html><h1>I 💗 Cortex!</h1></html>"
    #>
    [CmdletBinding()]
    param(
        # HTML string to convert
        [Parameter(Mandatory=$true,ValueFromPipeline=$true)][String]$Html
    )

    Begin {}
    Process {

        $ThisGuid = [System.Guid]::NewGuid().ToString()
        $tempHtmlfile = "$env:TEMP\Topics-Helper-$ThisGuid.htm"
        "<!DOCTYPE html><html>$Html</html>" | Out-File $tempHtmlfile

        # Oh yeah, we are going there
        $word = new-object -com Word.Application
        $word.visible = $true

        # Open that temp document
        [void]$word.Documents.Open($tempHtmlfile)

        # Save as Unicode text and release the document
        $saveFormat = 7 # wdFormatUnicodeText
        $tempTextfile = "$env:TEMP\Topics-Helper-$ThisGuid.txt"
        $word.ActiveDocument.SaveAs([ref]$tempTextfile,[ref]$saveFormat)
        $word.ActiveDocument.Close()

        # Bye, Word
        $word.Quit()

        $TextValue = Get-Content $tempTextfile | Out-String
        Remove-Item $tempTextfile,$tempHtmlfile

        return $TextValue
    }
    End {}
}


function HandleRestError () {
    <#
    .SYNOPSIS
     Response error throttle handler
    .DESCRIPTION

    .EXAMPLE
     HandleRestError -Error $_ -Retry $retry
    #>

    param(
        # Response headers object populated by the ResponseHeadersVariable pameters to the Invoke-RestMethod API call.
        [Parameter(Mandatory=$true)][System.Object[]]$Error,
        # Which retry is this?
        [Parameter(Mandatory=$true)][Int32]$Retry,
        # ResponseHeaders variable
        [Parameter(Mandatory=$false)][hashtable]$ResponseHeaders
    )

    $response = $Error.Exception.Response
    if ($response) {
        $requestId = foreach ($key in ($Response.Headers.GetEnumerator() | Where-Object { $_.Key -eq "Request-Id" })) { $Key.Value }        
    } else {
        Write-Host "Unexpected format of response for error handling"
        Write-Host $response
        return
    }

    Write-Verbose $response

    if (($response.StatusCode -eq 429 ) -or ($response.StatusCode -eq 500) -or ($response.StatusCode -eq 502)) {
        # We're getting throttled (429) or getting one of those spurious restart (500) or Bad Gateway (502) responses; back off and try again

        if ($response.StatusCode -eq 500) {
            $response.headers | out-string | write-host
            $_[0].ErrorDetails.Message | Out-String | Write-Host
        }

        $backoff = $Retry * 15

        Write-Warning "$([DateTime]::UtcNow.ToString('u')) : Retry $Retry - Service throttling response '$($response.StatusCode.ToString())', Request-Id '$requestId'. Sleeping $backoff seconds..."
        $Retry++

        Start-Sleep -seconds $backoff
    }
    else {
        Write-Warning "Request Uri: $($response.RequestMessage.RequestUri.AbsoluteUri)"
        Write-Warning "HTTP response: $($response.statuscode.value__) `"$($response.ReasonPhrase)`""
        Write-Host "Response headers:"
        $response.headers | out-string | write-host
      throw $_
    }
}


# Print debug information
function Print-DebugInfo ()
{
    <#
    .SYNOPSIS
     Print debug information
    .DESCRIPTION

    .EXAMPLE
     Print-DebugInfo $ResponseHeaders
    #>
    [CmdletBinding()]
    param(
        # Response headers object populated by the ResponseHeadersVariable pameters to the Invoke-RestMethod API call.
        [Parameter(Mandatory=$true)][hashtable]$ResponseHeaders,
         # Whether to use the Microservice instead of the default Substrate API.  Not recommended.
        [Parameter(Mandatory=$false)][switch]$UseMicroservice
   )

   Write-Verbose "`n------------------- Debug Information -------------------";
   if ($UseMicroservice) {
     Write-Verbose "Date: $($ResponseHeaders["Date"]))";
     Write-Verbose "Content-Type: $($ResponseHeaders["Content-Type"])";
     Write-Verbose "MS-CV: $($ResponseHeaders["MS-CV"])";
     Write-Verbose "SPHome-CV: $($ResponseHeaders["SPHome-CV"])";
     Write-Verbose "SPHome-Server: $($ResponseHeaders["SPHome-Server"])";
   } else {
     Write-Verbose "Request-Id: $($ResponseHeaders.$SubstrateRequestId)";
     Write-Verbose "X-CalculatedBETarget: $($ResponseHeaders.$SubstrateBETarget)";
     Write-Verbose "X-FEServer: $($ResponseHeaders.$SubstrateFEServer)";
     Write-Verbose "Date: $($ResponseHeaders.$Date))";
   }
   Write-Verbose "---------------------------------------------------------`n";
}