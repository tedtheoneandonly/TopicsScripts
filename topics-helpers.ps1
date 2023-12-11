<#
.SYNOPSIS
Topics-Helpers.ps1 contains several cmdlets that allow for interation with the Topics System
.NOTES
These functions and many of the APIs called are subject to change and aren't supported.
Use at your own risk.

Usage:

import-module .\topics-helpers.ps1 -Force
#>

. $PSScriptRoot\common.ps1

enum TopicFeedbackStatusType {
  Confirmed
  Rejected
}

enum TopicListType {
  Suggested
  UserSuggested
  Confirmed
  Published
  Removed
  All
  UX
}

enum SortDirectionType {
  Ascending = 1
  Descending = 2
}

enum SortFieldType {
  ConfirmedBy
  DiscoveredDateTime
  PublishedDateTime
  ImpressionCount
  Name
  Quality
  RejectedBy
  # New List only
  Impressions
  Modified
  ModifiedBy
  Created
}

enum TopicActionType {
  Confirm
  Remove
}

enum ConversationType {
  Teams
  TeamsChannel
  Yammer
  Outlook
  SpoSite
}

enum ClientType {
  Yammer
}

#region ################################################## Caching commandlets ##################################################
function Get-RestCache () {
  <#
    .SYNOPSIS
    Returns the current rest cache.
    .EXAMPLE
    $myCache = Get-RestCache
    .OUTPUTS
    The rest cache as a dictionary
    #>
  if (!$restCache) {
    return @{}
  }

  return $restCache
}

$restCache = Get-RestCache

function Set-RestCache () {
  <#
    .SYNOPSIS
    Sets the current rest cache.
    .EXAMPLE
    Set-RestCache $myCache
    #>
  [CmdletBinding()]
  param (
    [Parameter(Mandatory = $true)][Object] $cache
  )

  $restCache = $cache
}
#endregion

function Invoke-RestMethod-Cached () {
  <#
    .SYNOPSIS
    Calls Invoke-RestMethod with the parameters given, unless the call has been previously cached.
    .DESCRIPTION
    Calls are cached as files.
    .OUTPUTS
    The http response.
    #>
  [CmdletBinding()]
  param (
    # Token to test for time-validity
    [Parameter(Mandatory = $false)][Switch]$EnableCache,
    [Parameter(Mandatory = $true)][Microsoft.PowerShell.Commands.WebRequestMethod]$Method,
    [Parameter(Mandatory = $true)][Uri]$Uri,
    [Parameter(Mandatory = $true)][Microsoft.PowerShell.Commands.WebAuthenticationType]$Authentication,
    [Parameter(Mandatory = $true)][SecureString]$Token,
    [Parameter(Mandatory = $true)][System.Collections.IDictionary]$Headers,
    [Parameter(Mandatory = $false)][System.Object]$Body = "",
    [Parameter(Mandatory = $false)][string]$CacheRoot = ".\httpCache",
    [Parameter(Mandatory = $false)][string]$ResponseHeadersVariable = "ResponseHeadersVariable",
    [Parameter(Mandatory = $false)][string]$logInteractionsTo,
    [Parameter(Mandatory = $false)][Switch]$SkipCertificateCheck,
    [Parameter(Mandatory = $false)][int]$TimeoutSec = 120
  )

  $cache = Get-RestCache

  $hash = $Uri.GetHashCode()
  if ($Body) { $hash += ($Body | ConvertTo-Json).GetHashCode() }
  if ($Headers -and ($Headers.Count -gt 0)) { $hash += ($Headers.GetEnumerator() | Where-Object { $_.Name -ne "Client-Request-Id" } | ConvertTo-Json).GetHashCode() }

  if ($EnableCache -and ($cache[$hash])) {
    $result = $cache[$hash]

  }
  else {
    try {
      if ($VerbosePreference -ne "SilentlyContinue") {
        Write-Verbose "Invoke-RestMethod -Method $Method -Uri $Uri -Authentication $Authentication -Token $Token -Headers $Headers -Body $Body -TimeoutSec:$TimeoutSec -ResponseHeadersVariable:$ResponseHeadersVariable -SkipCertificateCheck:$SkipCertificateCheck"
      }
      $result = Invoke-RestMethod -Method $Method -Uri $Uri -Authentication $Authentication -Token $Token -Headers $Headers -Body $Body -TimeoutSec:$TimeoutSec -ResponseHeadersVariable:$ResponseHeadersVariable
    }
    catch {
      Write-Host "Exception: $($_.Exception.Message)"
      Write-Host "Http Status: $($_.Exception.Response.StatusCode)"
      if ($_.Exception.Response) {
          Write-Host "Request Id: $($_.Exception.Response.Headers["Request-Id"])"
      }
    }
    if (!$result) {
      "Exception in Invoke-RestMethod-Cached()" | Write-Debug
      $uri | Write-Debug
      Write-Host $Uri
    
    }
    else {
      if ($EnableCache) {
        $cache[$hash] = $result
      }
      else {
        "Did not cache request because caching is turned off" | Write-Verbose
      }
    }
  }

  if ($logInteractionsTo) {
    $io = [PSCustomObject]@{
      Method         = $Method
      RequestUrl     = $Uri
      RequestHeaders = $Headers
      RequestBody    = $Body
      Response       = $result
    }

    $io | ConvertTo-Json -EscapeHandling:EscapeNonAscii -Compress -Depth:10 | Add-Content -Path $logInteractionsTo
  }

  return $result;
}
#endregion

#region ################################################## Viva Topic commandlets ##################################################
function Get-TopicList () {
  <#
    .SYNOPSIS
     Retrieves Topic lists via /api/v1.0/Topics/Managed.  Default topicListType is Suggested,
     sortField is ImpressionCount, sortDirection is Descending and Count is unlimited.
    .EXAMPLE
     Get-TopicList -topicListType:Suggested
     Get-TopicList -topicListType:Published -sortField:TopicName -Count:10 -sortDirection:Ascending
    #>

  param(
    # If present, the http calls will be cached.
    [Parameter(Mandatory = $false)][Switch]$EnableCache,
    # Topic type - Suggested, Confirmed, Published, Removed, All, UX
    [Parameter(Mandatory = $false)][TopicListType]$topicListType,
    # Sort field - ConfirmedBy, DiscoveredDateTime, ImpressionCount, TopicName, TopicQualityScore
    [Parameter(Mandatory = $false)][SortFieldType]$sortField = [SortFieldType]::Name,
    # Ascending or Descending
    [Parameter(Mandatory = $false)][SortDirectionType]$sortDirection = [SortDirectionType]::Descending,
    # pipeline id
    [Parameter(Mandatory = $false)][PipelineType]$pipeline = [PipelineType]::Live,
    # Only return topics that start with this text
    [Parameter(Mandatory = $false)][string]$StartsWith = [string]::Empty,
    # Maximum number of topics to return
    [Parameter(Mandatory = $false)][System.Int32]$Count = 0,
    # Return the ID property of topics
    [Parameter(Mandatory = $false)][Switch]$IdOnly,
    # Optional token to specify.  Default is token obtained for current authentication context.
    [Parameter(Mandatory = $false)][SecureString]$token = $emptySecureString,
    # Languages of topics to return. Defaults to English. Can select multiple languages using commas (i.e. en,es,de).
    [Parameter(Mandatory = $false)][string]$Languages = "en",
    # Check whether we want topics with or without Definitions
    [Parameter(Mandatory = $false)][DefinitionType]$DefinitionType = [DefinitionType]::All
  )

  # Set defaults

  $maxRetry = 10

  if ($null -eq $topicListType) {
    $topicListType = [TopicListType]::All
  }

  $topicsPerPage = 10000

  if ($token -eq $emptySecureString) {
    $token = ValidateAndSetToken -token $token -tokenUri $SubstrateUri
  }

  AddSubstrateRouteHeaders -token $token

  if ($pipeline -ne [PipelineType]::Live) {
    # Get that debug/test pipeline
    $headers["X-Debug-EnableVivaTopicsCenterV3"] = "true"
    $headers["X-SPDF-TestFeedback"] = "true"
  }
  
  $headers["X-Debug-Enablesourcesfortopiccenter"] = "true"
  $headers["X-Debug-Enablesourcesv3fortopiccenter"] = "true"
  $headers["X-Debug-Enabletopicscorenormalizationv2"] = "true"
  $headers["X-Debug-Serversidepaginationenabledheader"] = "true"
  $headers["X-Odataquery"] = "true"
  
  $parsedToken = $token | Parse-JWTtoken

  # Latest topic count from the dashboard
  $latestSnapshot = 0

  # Prefix match on topic name or alternate names
  if ($StartsWith -ne [string]::Empty) {
    $topicNameStartsWith = "`"TopicNameStartsWith`":`"$StartsWith`","
  }
  else {
    $topicNameStartsWith = ""
  }

  # Declare KM API namespace for Managed Topics lists
  $topicUri = "$SubstrateUri/KnowledgeGraph/api/v1.0/Topics/Managed"

  $headers["X-Scenario"] = "PowerShell.Commandlet.Script"
  $headers["X-Accept-Language"] = $Languages

  try {
    $curPage = 1 
    $curPos = 0
    $totalTopics = 0

    # if user specifies count of topics to retrieve, make that the upper-bound,
    # else use the latest topic count from the dashboard
    if ($Count -gt 0) {
      $totalTopics = $Count
      if ($Count -lt $topicsPerPage) {
        $topicsPerPage = $Count
      }
    }
    else {
      $latestSnapshot = Get-TopicsDashboard -pipeline:$pipeline -token $token
      $suggestedTopics = $latestSnapshot.Suggested | Select-Object -Last 1
      $confirmedTopics = $latestSnapshot.Confirmed | Select-Object -Last 1
      $publishedTopics = $latestSnapshot.Published | Select-Object -Last 1
      $removedTopics = $latestSnapshot.Removed | Select-Object -Last 1

      # Handle diff between old and new list type names
      if (@([TopicListType]::All, [TopicListType]::UX) -contains $topicListType) {
        $totalTopics += $null -ne $suggestedTopics.Value ? $suggestedTopics.Value : $topicsPerPage
        $totalTopics += $null -ne $confirmedTopics.Value ? $confirmedTopics.Value : $topicsPerPage
        $totalTopics += $null -ne $publishedTopics.Value ? $publishedTopics.Value : $topicsPerPage
      }
      elseif ($topicListType -eq [TopicListType]::Suggested) {
        $totalTopics += $null -ne $suggestedTopics.Value ? $suggestedTopics.Value : $topicsPerPage
      }
      elseif ($topicListType -eq [TopicListType]::Confirmed) {
        $totalTopics += $null -ne $confirmedTopics.Value ? $confirmedTopics.Value : $topicsPerPage
      }
      elseif ($topicListType -eq [TopicListType]::Published) {
        $totalTopics += $null -ne $publishedTopics.Value ? $publishedTopics.Value : $topicsPerPage
      }
      elseif ($topicListType -eq [TopicListType]::Removed) {
        $totalTopics += $null -ne $removedTopics.Value ? $removedTopics.Value : $topicsPerPage
      }
    }

    Write-Verbose "Getting $totalTopics topics."

    $paginationToken = ""

    $percentComplete = 0
    $nextPage = $topicsPerPage

    # This can take a while, so start some counters that we'll use in the PowerShell Write-Progress indicator
    $totalTimer = [System.Diagnostics.Stopwatch]::StartNew()
    $pageTimer = [System.Diagnostics.Stopwatch]::StartNew()
    $lastPageTime = 0
    $retry = 1
    $getMoreTopics = $true

    # Page through collection; would prefer a skipToken over this paging scheme
    #while ($getMoreTopics) ($curPage -ne 0) -or ($true -eq $data.IsMoreTopicsAvailable)) {
    while ($getMoreTopics) {
      # Fetch some topics
      if ($__psEditorServices_CallStack.Command -contains "Get-KnowledgeBase") {
        $gettingTopics = "Getting topics"
      }
      else {
        $gettingTopics = "Getting topic list"
      }
      $activityText = "$gettingTopics (as $($null -ne $parsedToken.upn ? $parsedToken.upn : $parsedToken.smtp ), tenant $($parsedToken.tid)): `"$topicListType`" topics, sorted by `"$sortField`", $sortDirection"
      $avgPageTime = [Math]::Round($pageTimer.ElapsedMilliseconds / $curPage)

      $currentOperationText = "Req $curPage`: Topics $($curPos)-$($curPos + $nextPage - 1) of $($totalTopics - 1), avg $avgPageTime ms / $topicsPerPage items. " +
      "Last request $($totalTimer.ElapsedMilliseconds - $lastPageTime) ms. Elapsed time $([Math]::Floor($totalTimer.ElapsedMilliseconds / 1000))s." +
      " $(($retry -gt 1) ? "Retry #$retry" : [String]::Empty)"

      $lastPageTime = $totalTimer.ElapsedMilliseconds

      Write-Progress -Id 1 -ParentId 0 -Activity $activityText -CurrentOperation $currentOperationText -Status "Retrieved $curPos out of $totalTopics topics" -PercentComplete $percentComplete

      if ($topicListType -eq [TopicListType]::All) {
        $newTopicListFilter = "`"Suggested`",`"Confirmed`",`"Published`",`"Removed`""
      }
      elseif ($topicListType -eq [TopicListType]::UX) {
        $newTopicListFilter = ""
      }
      elseif ($newTopicListFiler -eq [TopicListType]::Suggested) {
        $newTopicListFilter = "`"Suggested`""
      }
      elseif ($newTopicListFiler -eq [TopicListType]::Removed) {
        $newTopicListFilter = "`"Removed`""
      }
      else {
        $newTopicListFilter = "`"$topicListType`""
      }
      
      $definitionText = "`"$DefinitionType`""
      if ($DefinitionType -eq [DefinitionType]::All) {
          $definitionText = "`"Present`",`"Missing`""
      }

      $postBody = "{`"Size`": $topicsPerPage,`"PaginationToken`":`"$paginationToken`",`"SortBy`":[{`"Field`":`"$sortField`",`"Direction`":`"$(($sortDirection -eq [SortDirectionType]::Descending) ? "Desc" : "Asc")`"}],`"Filters`":{`"LifeCycleStates`":[$newTopicListFilter],`"TopicNameStartsWith`":`"$StartsWith`",`"DefinitionStates`":[$definitionText],`"Sites`":[],`"Sources`":`"`"}}"

      $token = Get-UserToken -TokenToRenew:$token
      $token = ValidateAndSetToken -token $token -tokenUri $SubstrateUri

      if ($VerbosePreference -ne "SilentlyContinue") {
        Write-Verbose "HTTP request headers"
        foreach ($header in $headers.Keys) { Write-Verbose "$header`: $($headers[$header])" }
      }
      Write-Verbose "HTTP POST body: $PostBody"

      $data = $null

      while (($retry -lt $maxRetry) -and ($null -eq $data)) {
        # HTTP / API retry loop
        try {
          $headers["Client-Request-Id"] = [System.Guid]::NewGuid().ToString()
          $headers["X-Debug-ServerSidePaginationEnabledHeader"] = "true"
          $data = Invoke-RestMethod-Cached -Method POST -Uri $topicUri -Authentication Bearer -Token $token -Headers $headers -Body $PostBody -EnableCache:$EnableCache -ResponseHeadersVariable ResponseHeaders
        }
        catch {
          HandleRestError -Error $_ -Retry $retry
        }

        if (!$data) {
          $token = ValidateAndSetToken -token $token -tokenUri $SubstrateUri
          $backoff = $retry * 15
          $retry++
          Write-Progress -Id 102 -Activity "Retry $retry. Sleeping $backoff seconds..." -ParentId 1
          Write-Host "Sleeping $backoff seconds..."
          Start-Sleep -seconds $backoff
          Write-Progress -Id 102 -Activity "Retry $retry. Sleeping $backoff seconds..." -ParentId 1 -Completed
        }
      }

      # Check that we got some rows back
      $topicCount = $null -ne $data.value ? $data.value.Length : $data.TopicCenterTopics.Count
      Write-Host "Records returned $($topicCount)"

      # if there are any topics returned
      if ($topicCount -ne 0) {
        # grab topics from both old and new API
        $topicData = $null -ne $data.value ? $data.value : $data.TopicCenterTopics

        # Add them to the topics ArrayList
        if ($IdOnly) {
          # Outputs array of ID strings to pipeline to allow batch processing
          Write-Output @(, @($topicData.Id))
        }
        else {
          # Output topic objects to pipeline
          Write-Output $topicData
        }

        # Figure out what the next From value should be
        # if there are 1 or more topics, and the server says there are more available OR
        # if we aren't at max topics, and we got a full page
        if ((($topicCount -gt 0) -and ($true -eq $data.IsMoreTopicsAvailable)) -or ($curPos -le $totalTopics)) {
          # Move forward by count of topics returned
          $curPos += $topicCount
          # Increment page counter
          $curPage++
          # Update that progress indicator
          $percentComplete = ($curPos / $totalTopics) * 100

          #Reset any retries
          $retry = 1

          # Check we're on the last page of content
          if (($curPos + $nextPage) -gt $totalTopics) {
            # Shrink next page to ask for current to max known
            $nextPage = $totalTopics - $curPos
          }
        }
        elseif ($topicCount -gt 0) {
          # We got some data, but no more left to ask for, wrap it up
          $percentComplete = 100
          $getMoreTopics = $false
          $curPage = 0
        }

        # Fix syntax based on old vs new API
        $fromCount = "`"From`":$curPos"
        $paginationToken = $data.PaginationToken.Replace('"', '\"')

        # Check to see if we're going past end-of-list or if API tells us that there's no more left to fetch
        if ($curPos -ge $totalTopics) {
          # Done!
          $percentComplete = 100
          $getMoreTopics = $false
          $curPage = 0
        }

        $retry = 1
      }
      elseif ($true -eq $data.IsMoreTopicsAvailable) {
        # Something funky happened we need to retry same page
        Write-Verbose "Unexpected zero results w/ more data on server)"
        Write-Verbose "Request-id: $($ResponseHeaders.RequestId)"
        Write-Verbose "Retry count $retry"
        Write-Verbose $data | ConvertTo-Json
        Write-Warning "Zero response, more on server. Retry $retry. From:$curPos Size:$nextPage. Response Request-Id: $($responseHeaders.RequestId)"
        Write-Warning $PostBody

        if ($VerbosePreference -ne "SilentlyContinue") {
          foreach ($header in $ResponseHeaders.Keys) {
            Write-Verbose "$header`: $($ResponseHeaders[$header])"
          }
        }

        if ($retry -gt $maxRetry) {
          # Wrap it up
          $percentComplete = 100
          $getMoreTopics = $false
          $curPage = 0
        }
        else {
          # Don't move forward, retry current page
          $retry++
          Start-Sleep -seconds 1
        }
      }
      else {
        # Done - zero results means no more to get.  @odata.NextLink is in our future!
        $percentComplete = 100
        $getMoreTopics = $false
        $curPage = 0
      }
    }
    $pageTimer.Stop()
    $totalTimer.Stop()
  }
  catch {
    $_
  }
  Write-Progress -Id 1 -ParentId 0 -Activity "f" -Complete
  Write-Verbose "Elapsed: $([Math]::Round($totalTimer.ElapsedMilliseconds / 1000)) seconds"

  return
}

function Get-KnowledgeBase () {
  <#
    .SYNOPSIS
     Retrieves tenant knowledge base

    .EXAMPLE
     Get-KnowledgeBase  | Export-Clixml .\MyKB.xml
    #>

  [CmdletBinding()]
  param (
    # Batch size for requesting topic data
    [Parameter(Mandatory = $false)][Int]$batchSize = 150,
    # Filename to use for topic ID cache.  Existing file will be used; file specified with otherwise be created.
    [Parameter(Mandatory = $false)][String]$TopicListCache = $null,
    # pipeline id
    [Parameter(Mandatory = $false)][PipelineType]$pipeline = [PipelineType]::Live,
    # If true, use the feedback test pipeline debug header (required to work within MSIT)
    [Parameter(Mandatory = $false)][boolean]$Test = $true,
    # Optional token to specify.  Default is token obtained for current authentication context.
    [Parameter(Mandatory = $false)][SecureString]$token = $emptySecureString,
    # Use new listing API
    [Parameter(Mandatory = $false)][switch]$NewList = $false,
    # Use Eval commands
    [Parameter(Mandatory = $false)][switch]$EvalTopics = $false,
    # If present, the http calls will be cached.
    [Parameter(Mandatory = $false)][Switch]$EnableCache
  )

  $orgKb = @()
  $orgKbDetails = @()

  $token = ValidateAndSetToken -token $token -tokenUri $SubstrateUri
  AddSubstrateRouteHeaders -token $token

  if ($NewList) {
    Get-TopicList -NewList -idonly -sortField:TopicName -sortDirection:Ascending -pipeline $pipeline -Token $token -EnableCache:$EnableCache | Get-Topic -pipeline $pipeline -Token $token -EnableCache:$EnableCache
    return
  }
  if (Test-Path $TopicListCache) {
    # There's a cached list of topics to retrieve, go get it
    $orgKb = Import-Clixml $TopicListCache
  }
  if ($EvalTopics) {
    $orgKb += Get-EvalTopicList -Token $token -pipeline $pipeline # TODO: -EnableCache:$EnableCache
  }
  else {
    $orgKb += Get-TopicList -topicListType:Suggested -Token $token -pipeline $pipeline -EnableCache:$EnableCache
    $orgKb += Get-TopicList -topicListType:Published -Token $token -pipeline $pipeline -EnableCache:$EnableCache
    $orgKb += Get-TopicList -topicListType:Confirmed -Token $token -pipeline $pipeline -EnableCache:$EnableCache
    $orgKb += Get-TopicList -topicListType:Removed -Token $token -pipeline $pipeline -EnableCache:$EnableCache
  }

  if ($TopicListCache -ne "") {
    $orgKb | Export-Clixml $TopicListCache
  }

  $curPos = 0
  $avgPageTime = 0

  $pageTimer = [System.Diagnostics.Stopwatch]::StartNew()
  $curPage = 1

  do {
    # Check and bump
    $token = Get-UserToken -TokenToRenew:$token

    if ($orgKb.Count -ge ($curPos + $batchSize - 1)) {
      Write-Progress -id 909 -Activity "Fetching topics $curPos to $($curPos + $batchSize - 1) of $($orgKb.Count) - $avgPageTime ms/topic average" -PercentComplete ((($curPos + 1) / $orgKb.Count) * 100)

      $avgPageTime = [Math]::Round(($pageTimer.ElapsedMilliseconds / $curPage) / $batchSize)

      #$orgKbDetails += Get-Topic $orgKb[$curPos..($curPos + $batchSize-1)].Id -pipeline $pipeline -Token $token
      # output topic objects to pipeline
      Get-Topic $orgKb[$curPos..($curPos + $batchSize - 1)].Id -pipeline $pipeline -Token $token -EnableCache:$EnableCache

      $curPos += $batchSize
      $curPage++
    }
    else {
      Write-Progress -id 909 -Activity "Fetching topics $($curPos + 1) to $($curPos + $batchSize - 1) of $($orgKb.Count) - $avgPageTime ms/topic average" -PercentComplete (($curPos + 1) / $orgKb.Count * 100)
      $avgPageTime = [Math]::Round(($pageTimer.ElapsedMilliseconds / $curPage) / $batchSize)

      #$orgKbDetails += Get-Topic $orgKb[$curPos..($orgKb.Count - 1)].Id -pipeline $pipeline -Token $token
      # output topic objects to pipeline
      Get-Topic $orgKb[$curPos..($orgKb.Count - 1)].Id -pipeline $pipeline -Token $token -EnableCache:$EnableCache

      $curPos = $orgKb.Count
      $curPage++
    }

  } while ($curPos -lt $orgKb.Count)
  $pageTimer.Stop()

  # Get total average
  $avgPageTime = [Math]::Round($pageTimer.ElapsedMilliseconds / $curPage)

  Write-Host "Batch size $batchSize, $avgPageTime ms average per page. Total $($orgKb.Count) topics retrieved in $($pageTimer.ElapsedMilliseconds) ms."

  Write-Progress -id 909 -Activity "Fetching" -Completed
  return
}

function Get-Topic () {
  <#
    .SYNOPSIS
     Fetch Topic object by ID
    .DESCRIPTION

    .EXAMPLE
     Get-Topic AL_hYazT2L8PDI844HtHQhRTQ
     Get-Topic AL_hYazT2L8PDI844HtHQhRTQ,AL_--5-sHEQyswzwZ2D2ZWT1w
    #>
  [CmdletBinding()]
  param(
    # One or more IDs of topics, comma-delimited
    [Parameter(Mandatory = $true, ValueFromPipeline = $true, ValueFromPipelineByPropertyName = $true)][ValidateCount(1, 1024)][String[]]$Id,
    # pipeline id
    [Parameter(Mandatory = $false)][PipelineType]$pipeline = [PipelineType]::Live,
    # Optional number of files to retrieve for topic (otherwise API default will be applied)
    [Parameter(Mandatory = $false)][int]$numFiles,
    # Optional number of FAQs to retrieve for topic (otherwise none will be requested)
    [Parameter(Mandatory = $false)][int]$numFaqs = 0,
    # Optional number of retries on error/empty result
    [Parameter(Mandatory = $false)][int]$numRetries = 8,
    # Optional token to specify.  Default is token obtained for current authentication context.
    [Parameter(Mandatory = $false)][SecureString]$token = $emptySecureString,
    # If true, an http cache will be applied.
    [Parameter(Mandatory = $false)][Switch]$EnableCache
  )

  Begin {
    $token = ValidateAndSetToken -token $token -tokenUri $SubstrateUri
    AddSubstrateRouteHeaders -token $token
  }

  Process {

    # todo - cleanup duplicate
    $token = ValidateAndSetToken -token $token -tokenUri $SubstrateUri
    AddSubstrateRouteHeaders -token $token
    # end todo

    if ($Id.Count -gt 0) {
      # Need to assemble payload
      $topicUri = "$SubstrateUri/KnowledgeGraph/api/v1.0/Topics/Ids?pipeline=" + $pipeline
      # Build body
      $PostBody = "{`"TopicIds`":["

      if ($null -ne $_.Id) {
        $PostBody += "`"$($_.Id)`""
      }
      else {
        $i = 1
        foreach ($idLocal in $Id[0..($Id.Count - 1)]) {
          $PostBody += "`"$idLocal`""
          if ($i -ne $Id.Length) {
            $PostBody += ","
          }
          $i++
        }
      }
      $PostBody += "]"
      if ($numFiles) {
        $PostBody += ",`"ResourceOptions`":{`"FileCount`":$numFiles}"
      }
      if ($numFaqs -gt 0) {
        $headers["X-Debug-Enabletopicfaq"] = "true"
        $PostBody += ",`"FaqOptions`":{`"MaxFaqsPerTopic`":$numFaqs,`"IsFaqsOnly`":true}"
      }
      
      $PostBody += "}"

      # Go get those topics
      $data = $null
      $retry = 1

      while (($retry -ne $numRetries + 2) -and ($null -eq $data)) {
          $headers["Client-Request-Id"] = [System.Guid]::NewGuid().ToString()
          $headers["X-Accept-Language"] = "MIXED"
          $headers["X-Debug-FilterTopicLiteTopics"] = "false"
          Write-Verbose "Url: $topicUri"
          Write-Verbose "PostBody: $PostBody"
          
          $data = Invoke-RestMethod-Cached -Method POST -Uri $topicUri -Authentication Bearer -Token $token -Headers $headers -Body $PostBody -ResponseHeadersVariable ResponseHeaders -EnableCache:$EnableCache
          if ($ResponseHeaders) { Write-Verbose "Request-id: $($ResponseHeaders["request-id"])" }

        if (!$data) {
          $token = ValidateAndSetToken -token $token -tokenUri $SubstrateUri
          $backoff = $retry * 15
          $retry++
          Write-Progress -Id 102 -Activity "Retry $retry. Sleeping $backoff seconds..." -ParentId 1
          Write-Host "Sleeping $backoff seconds..."
          Start-Sleep -seconds $backoff
          Write-Progress -Id 102 -Activity "Retry $retry. Sleeping $backoff seconds..." -ParentId 1 -Completed
        }
      }
      if ($data.value.Length -ne 0) {
        Write-Output $data.value
      }
    }

    return
  }
  End {
    if ($null -ne $ResponseHeaders) {
      Print-DebugInfo $ResponseHeaders
    }
  }
}

function Set-Topic () {
  <#
    .SYNOPSIS
    Perform actions on topics
    #>
  [CmdletBinding(SupportsShouldProcess, ConfirmImpact = 'High')]
  param(
    # One or more IDs of topics to act on
    [Parameter(Mandatory = $true, ValueFromPipeline = $true)][ValidateCount(1, 1024)][String[]]$Id,
    # Choice of Confirm or Remove
    [Parameter(Mandatory = $true)][TopicActionType]$Action,
    # Optional token to specify.  Default is token obtained for current authentication context.
    [Parameter(Mandatory = $false)][SecureString]$token = $emptySecureString,
    # Option token to automatically accept all Confirm/Remove items
    [Parameter(Mandatory = $false)][bool]$autoApprove = $false
  )

  Begin {
    $token = ValidateAndSetToken -token $token -tokenUri $SubstrateUri
    AddSubstrateRouteHeaders -token $token
  }

  Process {

    $headers["Client-Request-Id"] = [System.Guid]::NewGuid().ToString()

    try {
      if ($Id.Count -gt 0) {

        # Need to assemble payload
        switch ($Action) {
          Confirm {
            $topicUri = "$SubstrateUri/KnowledgeGraph/api/v1.0/Topics/Confirm"
          }
          Remove {
            $topicUri = "$SubstrateUri/KnowledgeGraph/api/v1.0/Topics/Exclude"
          }
          Default {}
        }

        # Build body
        $PostBody = "{`"Topics`":["

        $i = 1
        $confirmCount = 0
        foreach ($item in $Id[0..($Id.Count - 1)]) {
          if ($null -ne $_.Type) {
            $topic = $_
          }
          else {
            $topic = Get-Topic -Id $item -token $token
          }

          if ($topic -eq $null) {
            Write-Warning "Topic with ID $($Id) not found."
            return;
          }

          if ($topic.LifeCycle.State -eq "Published" -and $Action -eq "Confirm") {
            Write-Warning "Topic with ID $($Id) is 'Published'. 'Confirm' action does not apply."
            return;
          }

          if ($autoApprove -or $PSCmdlet.ShouldProcess("Verbose: $Action",
              "`nName: `'$($topic.DisplayName)`', ID: $($topic.Id) `nCurrent state: `'$($topic.LifeCycle.State)`' `nDefinition: $($null -ne $topic.Definition ? $topic.Definition : "[None]")",
              "Do you want to $($Action.ToString().ToLower()) the following topic?" )) {
            # hardcoding in value for name if piped value isn't there
            $PostBody += "{`"Id`":`"$($null -ne $_.Id ? $_.Id : $item)`",`"Name`":`"$($null -ne $_.Name ? $_.Name : $topic.DisplayName)`"}"
            $confirmCount++
            if ($i -ne $Id.Length) {
              $PostBody += ","
            }
          }
          $i++
        }
        $PostBody += "]}"

        if ($confirmCount -eq 0) {
          Write-Warning "No topics to $($Action.ToString().ToLower())."
          return
        }

        if ($VerbosePreference -ne "SilentlyContinue") {
          Write-Verbose "HTTP request headers"
          foreach ($header in $headers.Keys) { Write-Verbose "$header`: $($headers[$header])" }
          Write-Verbose $PostBody
        }
        # Go set those topics
        try {
          # eat the response
          $data = Invoke-RestMethod -Method POST -Uri $topicUri -Authentication Bearer -Token $token -Headers $headers -Body $PostBody
          Write-Host "`nSuccess: Action: $Action on $PostBody"
        }
        catch {
          # throw
          $_
        }
      }
    }
    catch {
      # throw
      $_
    }
  }
  End {}
}

function Get-TopicByName () {
  <#
    .SYNOPSIS
     Fetch Topic object by name
    .DESCRIPTION

    .EXAMPLE
     Get-TopicByName "Foobar"
    #>
  [CmdletBinding()]
  param(
    # One or more IDs of topics, comma-delimited
    [Parameter(Mandatory = $true, ValueFromPipeline = $true)][ValidateCount(1, 1)][String[]]$Name,
    # Optional count of topics to return.  Default is 10.
    [Parameter(Mandatory = $false)][Int32]$TopicCount = 10,
    # Optional token to specify.  Default is token obtained for current authentication context.
    [Parameter(Mandatory = $false)][SecureString]$token = $emptySecureString
  )

  Begin {

    $token = ValidateAndSetToken -token $token -tokenUri $SubstrateUri
    AddSubstrateRouteHeaders -token $token
  }

  Process {
    return Get-TopicSuggestions -Name $Name -TopicCount $TopicCount -token $token -ExactMatch
  }
  End {}
}

function Get-TopicSuggestions () {
  <#
    .SYNOPSIS
     Fetch Topic objects by prefix
    .DESCRIPTION

    .EXAMPLE
     Get-TopicSuggestions "Foo"
     Get-TopicSuggestions "Foob"
     Get-TopicSuggestions "Foobar"
    #>
  [CmdletBinding()]
  param(
    # One or more IDs of topics, comma-delimited
    [Parameter(Mandatory = $true, ValueFromPipeline = $true)][ValidateCount(1, 1)][String[]]$Name,
    # Optional count of topics to return.  Default is 10.
    [Parameter(Mandatory = $false)][Int32]$TopicCount = 10,
    # Optional get exact match for the provided name
    [Parameter(Mandatory = $false)][Switch]$ExactMatch,
    # Optional token to specify.  Default is token obtained for current authentication context.
    [Parameter(Mandatory = $false)][SecureString]$token = $emptySecureString,
    # If true, the HTTP calls will be cached.
    [Parameter(Mandatory = $false)][Switch]$EnableCache,
    [Parameter(Mandatory = $false)][Hashtable]$SuggestionsRankerParameters
  )

  Begin {
    if ($token -eq $emptySecureString) {
      $token = ValidateAndSetToken -token $token -tokenUri $SubstrateUri
    }
    AddSubstrateRouteHeaders -token $token
  }

  Process {
    $topics = [System.Collections.ArrayList]::new()

    if ($Name.Count -gt 0) {

      if ($null -ne $_)
      {}
      else {
        $nameOrPrefix = $ExactMatch ? "Name" : "Prefix";
        # Build request params
        $UrlParam = "&$nameOrPrefix=$([System.Web.HttpUtility]::UrlEncode($Name))&topicCount=$TopicCount"
      }

      # Need to assemble payload
      $topicUri = "$SubstrateUri/KnowledgeGraph/api/v1.0/Topics?provider=Yggdrasil$UrlParam"

      # Go get those topics
      $data = $null
      $retry = 1

      while (($retry -ne 10) -and ($null -eq $data)) {
        try {
          $headers["Client-Request-Id"] = [System.Guid]::NewGuid().ToString()
          $headers["X-Accept-Language"] = "MIXED"
          $headers["X-Debug-FilterTopicLiteTopics"] = "false"
          if ($SuggestionsRankerParameters) {
            $rankerParameters = $SuggestionsRankerParameters | ConvertTo-Json -Compress
            $headers["X-Debug-SuggestionsRankerParameters"] = $rankerParameters
          }

          $headers["x-scenario"] = "SPPages.TopicPicker.SPPage"
          
          $data = Invoke-RestMethod-Cached -Method GET -Uri $topicUri -Authentication Bearer -Token $token -Headers $headers -Body $PostBody -ResponseHeadersVariable ResponseHeaders -EnableCache:$EnableCache
          $retry++
        }
        catch {
          HandleRestError -Error $_ -Retry $retry
        }
      }
      if ($data.value.Length -ne 0) {
        [void]$topics.AddRange($data.value)
      }
    }

    return $topics
  }
  End {
    if ($ResponseHeaders) {
      Print-DebugInfo $ResponseHeaders
    }
  }
}

function Get-TopicsDashboard() {
  <#
    .SYNOPSIS
     Retrieves Topic dashboard via /api/v1.0/Topics/ManagedDashboardV2.
    .EXAMPLE
     Get-TopicsDashboard
  #>

  param(
    [Parameter(Mandatory = $false)][SecureString]$token = $emptySecureString,
    # pipeline id
    [Parameter(Mandatory = $false)][PipelineType]$pipeline = [PipelineType]::Live,
    # Optional parameter to use the test pipeline
    [Parameter(Mandatory = $false)][Switch]$Test = $false
  )

  $token = ValidateAndSetToken -token $token -tokenUri $SubstrateUri
  AddSubstrateRouteHeaders -token $token

  if ($Test) {
    $headers[$SPDFTestHeader] = "true"
  }

  # KM API for Managed Dashboard
  $topicUri = "$SubstrateUri/KnowledgeGraph/api/v1.0/Topics/ManagedDashboardV2"

  # Request
  $PostBody = "{`"PastDays`":30 }"

  $headers["Client-Request-Id"] = [System.Guid]::NewGuid().ToString()

  $snapshots = [System.Collections.ArrayList]::new()

  try {
    if ($VerbosePreference -ne "SilentlyContinue") {
      Write-Verbose "HTTP request headers"
      foreach ($header in $headers.Keys) { Write-Verbose "$header`: $($headers[$header])" }
    }
    Write-Verbose "Post body: $PostBody"

    $data = Invoke-RestMethod -Method POST -Uri $topicUri -Authentication Bearer -Token $token -Headers $headers -Body $PostBody

    if (($null -ne $data) -and ($null -ne $data.Snapshots)) {
      return $data.Snapshots
    }
  }
  catch {
    Write-Error $_.Exception
    $_
  }

  return $snapshots
}

function Get-TopicMetrics () {
  <#
    .SYNOPSIS
     Fetch Topic Metrics
    .DESCRIPTION

    .EXAMPLE
     Get-TopicMetrics AL_hYazT2L8PDI844HtHQhRTQ
    #>
  [CmdletBinding()]
  param(
    # Id of topic.
    [Parameter(Mandatory = $true, ValueFromPipeline = $true)][String]$Id,
    # Pipeline Id.
    [Parameter(Mandatory = $false)][byte]$PipelineId = 3,
    # Tenant ID, defaults to script user's tenant if not specified.
    [Parameter(Mandatory = $false)][String]$TenantId,
    # Source tag (use to differentiate between different scenarios like SPO pages, Teams, Outlook, ...etc., defined by KMAPI).
    [Parameter(Mandatory = $false)][String]$Source,
    # Routes request to localhost.
    [Parameter(Mandatory = $false)][switch]$runLocal,
    # use fiddler proxy.
    [Parameter(Mandatory = $false)][switch]$UseFiddlerProxy,
    # Optional token to specify.  Default is token obtained for current authentication context.
    [Parameter(Mandatory = $false)][SecureString]$token = $emptySecureString
  )

  Begin {
    $token = ValidateAndSetToken -token $token -tokenUri $KMUri
    AddSubstrateRouteHeaders -token $token
  }

  Process {
    if ($useTestPipeline.IsPresent) {
      Write-Verbose "Using test pipeline by adding value 'TopicMetrics' to header request 'Debug-Mode'"
      $headers["Debug-Mode"] = "TopicMetrics"
    }
    else {
      $headers["Debug-Mode"] = ""
    }

    if ($null -ne $_.Id) {
      $Id = $_.Id
    }

    if ($runLocal.IsPresent) {
      $topicUri = $LocalUri
    }
    else {
      $topicUri = $KMUri
    }

    $topicUri += "/api/v1/TopicMetricsDiagnostics/metricsById?entityId=$Id&pipelineId=$PipelineId"

    if ($null -ne $Source) {
      $topicUri += "&source=$Source"
    }

    if ($null -ne $TenantId) {
      $topicUri += "&tenantId=$TenantId"
    }

    $headers["Client-Request-Id"] = [System.Guid]::NewGuid().ToString()
    $headers["SPHome-ClientType"] = "Diagnostics"

    try {
      if ($UseFiddlerProxy.IsPresent) {
        $data = Invoke-RestMethod -Method GET -Uri $topicUri -Authentication Bearer -Token $token -Headers $headers -Proxy $FiddlerProxy
      }
      else {
        $data = Invoke-RestMethod -Method GET -Uri $topicUri -Authentication Bearer -Token $token -Headers $headers
      }

      if ($null -ne $data) {
        $dataValue = $data.PSObject.Properties.Value
        if (0 -eq $dataValue) {
          return "No content found."
        }
        else {
          return $dataValue
        }
      }
    }
    catch {
      # throw
      $_
    }
  }
  End {}
}

function Get-TopicMetricsDirect () {
  <#
    .SYNOPSIS
     Fetch Topic Metrics directly from ObjectStore instead of SDK
    .DESCRIPTION

    .EXAMPLE
     Get-TopicMetricsDirect AL_hYazT2L8PDI844HtHQhRTQ
    #>
  [CmdletBinding()]
  param(
    # Id of topic.
    [Parameter(Mandatory = $true, ValueFromPipeline = $true)][String]$Id,
    # Pipeline Id.
    [Parameter(Mandatory = $false)][byte]$PipelineId = 3,
    # Source tag (use to differentiate between different scenarios like SPO pages, Teams, Outlook, ...etc., defined by KMAPI).
    [Parameter(Mandatory = $false)][String]$Source,
    # Routes request to localhost.
    [Parameter(Mandatory = $false)][switch]$runLocal,
    # use fiddler proxy.
    [Parameter(Mandatory = $false)][switch]$UseFiddlerProxy,
    # Optional token to specify.  Default is token obtained for current authentication context.
    [Parameter(Mandatory = $false)][SecureString]$token = $emptySecureString
  )

  Begin {
    $token = ValidateAndSetToken -token $token -tokenUri $KMUri
    AddSubstrateRouteHeaders -token $token
  }

  Process {
    if ($null -ne $_.Id) {
      $Id = $_.Id
    }

    if ($runLocal.IsPresent) {
      $topicUri = $LocalUri
    }
    else {
      $topicUri = $KMUri
    }

    $topicUri += "/api/v1/TopicMetricsDiagnostics/metricsByIdDirect?entityId=$Id&pipelineId=$PipelineId"

    if ($null -ne $Source) {
      $topicUri += "&source=$Source"
    }

    $headers["Client-Request-Id"] = [System.Guid]::NewGuid().ToString()
    $headers["SPHome-ClientType"] = "Diagnostics"

    try {
      if ($UseFiddlerProxy.IsPresent) {
        $data = Invoke-RestMethod -Method GET -Uri $topicUri -Authentication Bearer -Token $token -Headers $headers -Proxy $FiddlerProxy
      }
      else {
        $data = Invoke-RestMethod -Method GET -Uri $topicUri -Authentication Bearer -Token $token -Headers $headers
      }

      if ($null -ne $data) {
        $dataValue = $data.PSObject.Properties.Value
        if (0 -eq $dataValue) {
          return "No content found."
        }
        else {
          return $dataValue
        }
      }
    }
    catch {
      # throw
      $_
    }
  }
  End {}
}

function Register-TopicImpression () {
  <#
    .SYNOPSIS
     Register a topic impression
    .DESCRIPTION

    .EXAMPLE
     Register-TopicImpression AL_hYazT2L8PDI844HtHQhRTQ
    #>
  [CmdletBinding()]
  param(
    # Id of topic
    [Parameter(Mandatory = $true, ValueFromPipeline = $true)][String]$Id,
    # Source tag (use to differentiate between different scenarios like SPO pages, Teams, Outlook, ...etc., defined by KMAPI)
    [Parameter(Mandatory = $false)][String]$Source,
    # Pipeline Id
    [Parameter(Mandatory = $false)][byte]$PipelineId = 3,
    # Routes request to localhost.
    [Parameter(Mandatory = $false)][switch]$runLocal,
    # use fiddler proxy.
    [Parameter(Mandatory = $false)][switch]$UseFiddlerProxy,
    # Optional token to specify.  Default is token obtained for current authentication context.
    [Parameter(Mandatory = $false)][SecureString]$token = $emptySecureString
  )

  Begin {
    $token = ValidateAndSetToken -token $token -tokenUri $KMUri
    AddSubstrateRouteHeaders -token $token
  }

  Process {
    if ($useTestPipeline.IsPresent) {
      Write-Verbose "Using test pipeline by adding value 'TopicMetrics' to header request 'Debug-Mode'"
      $headers["Debug-Mode"] = "TopicMetrics"
    }
    else {
      $headers["Debug-Mode"] = ""
    }

    if ($null -ne $_.Id) {
      $Id = $_.Id
    }

    if ($runLocal.IsPresent) {
      $topicUri = $LocalUri
    }
    else {
      $topicUri = $KMUri
    }

    $topicUri += "/api/v1/TopicMetricsDiagnostics/impression?entityId=$Id&pipelineId=$PipelineId"

    if ($null -ne $Source) {
      $topicUri += "&source=$Source"
    }

    $headers["Client-Request-Id"] = [System.Guid]::NewGuid().ToString()
    $headers["SPHome-ClientType"] = "Diagnostics"

    try {
      if ($UseFiddlerProxy.IsPresent) {
        $data = Invoke-RestMethod -Method POST -Uri $topicUri -Authentication Bearer -Token $token -Headers $headers -Proxy $FiddlerProxy
      }
      else {
        $data = Invoke-RestMethod -Method POST -Uri $topicUri -Authentication Bearer -Token $token -Headers $headers
      }

      if ($null -ne $data) {
        return $data
      }
    }
    catch {
      # throw
      $_
    }
  }
  End {}
}

function Get-TopicFeedbackCount () {
  <#
    .SYNOPSIS
     Fetch Feedback count.
    .DESCRIPTION

    .EXAMPLE
     Get-TopicFeedbackCount
    #>
  [CmdletBinding()]
  param(
    # Pipeline Id.
    [Parameter(Mandatory = $false)][byte]$PipelineId = 3,
    # target region (if not set default home region will be used).
    [Parameter(Mandatory = $false)][String]$Region,
    # Routes request to localhost.
    [Parameter(Mandatory = $false)][switch]$RunLocal,
    # use fiddler proxy.
    [Parameter(Mandatory = $false)][switch]$UseFiddlerProxy,
    # Optional token to specify.  Default is token obtained for current authentication context.
    [Parameter(Mandatory = $false)][SecureString]$token = $emptySecureString
  )

  Begin {
    $token = ValidateAndSetToken -token $token -tokenUri $KMUri
    AddSubstrateRouteHeaders -token $token
  }

  Process {
    if ($RunLocal.IsPresent) {
      $topicUri = $LocalUri
    }
    else {
      $topicUri = $KMUri
    }

    $topicUri += "/api/v1/feedbackDiagnostics/feedbackCount?pipelineId=$PipelineId&region=$Region"

    $headers["Client-Request-Id"] = [System.Guid]::NewGuid().ToString()
    $headers["SPHome-ClientType"] = "Diagnostics"

    try {
      if ($UseFiddlerProxy.IsPresent) {
        $data = Invoke-RestMethod -Method GET -Uri $topicUri -Authentication Bearer -Token $token -Headers $headers -Proxy $FiddlerProxy
      }
      else {
        $data = Invoke-RestMethod -Method GET -Uri $topicUri -Authentication Bearer -Token $token -Headers $headers
      }

      if ($null -ne $data) {
        return [System.String]::Concat("Count=", $data)
      }
    }
    catch {
      # throw
      $_
    }
  }
  End {}
}

function Get-TopicFeedbackById () {
  <#
    .SYNOPSIS
     Fetch Feedback by Id.
    .DESCRIPTION

    .EXAMPLE
     Get-TopicFeedbackById AL_hYazT2L8PDI844HtHQhRTQ
    #>
  [CmdletBinding()]
  param(
    # Id of topic.
    [Parameter(Mandatory = $true, ValueFromPipeline = $true)][String]$Id,
    # ContextIds to check feedback for.
    [Parameter(Mandatory = $false)][String[]]$ContextIds,
    # Pipeline Id.
    [Parameter(Mandatory = $false)][byte]$PipelineId = 3,
    # target region (if not set default home region will be used).
    [Parameter(Mandatory = $false)][String]$Region,
    # the tenant Id
    [Parameter(Mandatory = $false)][String]$TenantId,
    # enable deep search for resource feedback.
    [Parameter(Mandatory = $false)][switch]$DeepSearchResourceFeedback,
    # Routes request to localhost.
    [Parameter(Mandatory = $false)][switch]$RunLocal,
    # use fiddler proxy.
    [Parameter(Mandatory = $false)][switch]$UseFiddlerProxy,
    # Optional token to specify.  Default is token obtained for current authentication context.
    [Parameter(Mandatory = $false)][SecureString]$token = $emptySecureString
  )

  Begin {
    $token = ValidateAndSetToken -token $token -tokenUri $KMUri
    AddSubstrateRouteHeaders -token $token
  }

  Process {
    if ($null -ne $_.Id) {
      $Id = $_.Id
    }

    if ($RunLocal.IsPresent) {
      $topicUri = $LocalUri
    }
    else {
      $topicUri = $KMUri
    }

    $topicUri += "/api/v1/feedbackDiagnostics/feedbackbyId?entityId=$Id&pipelineId=$PipelineId&region=$Region"

    if ($TenantId) {
      $topicUri += "&tenantId=$TenantId"
    }

    for ($i = 0; $i -lt $ContextIds.Count; $i++) {
      $contextId = $ContextIds[$i]
      $topicUri += "&contextIds[$i]=$contextId"
    }

    if ($DeepSearchResourceFeedback.IsPresent) {
      $topicUri += "&deepSearchForResourceFeedback=true"
    }

    $headers["Client-Request-Id"] = [System.Guid]::NewGuid().ToString()
    $headers["SPHome-ClientType"] = "Diagnostics"

    try {
      if ($UseFiddlerProxy.IsPresent) {
        $data = Invoke-RestMethod -Method GET -Uri $topicUri -Authentication Bearer -Token $token -Headers $headers -Proxy $FiddlerProxy
      }
      else {
        $data = Invoke-RestMethod -Method GET -Uri $topicUri -Authentication Bearer -Token $token -Headers $headers
      }

      if ($null -ne $data) {
        $dataValue = $data.PSObject.Properties.Value
        if (0 -eq $dataValue) {
          return "No content found."
        }
        else {
          $status = $dataValue.feedback.score.status
          if ($status -eq 0) {
            $status = "Undetermined"
          }
          elseif ($status -eq 1) {
            $status = "Confirmed"
          }
          elseif ($status -eq 2) {
            $status = "Rejected"
          }
          $dataValue.feedback.score.status = $status
          return $dataValue
        }
      }
    }
    catch {
      # throw
      $_
    }
  }
  End {}
}

function Get-TopicFeedbackByName () {
  <#
    .SYNOPSIS
     Fetch Feedback by name.
    .DESCRIPTION

    .EXAMPLE
     Get-TopicFeedbackByName Alexandria
    #>
  [CmdletBinding()]
  param(
    # Name of topic.
    [Parameter(Mandatory = $true, ValueFromPipeline = $true)][String]$Name,
    # ContextId to check feedback for.
    [Parameter(Mandatory = $false)][String]$ContextId,
    # Pipeline Id.
    [Parameter(Mandatory = $false)][byte]$PipelineId = 3,
    # target region (if not set default home region will be used).
    [Parameter(Mandatory = $false)][String]$Region,
    # Routes request to localhost.
    [Parameter(Mandatory = $false)][switch]$RunLocal,
    # use fiddler proxy.
    [Parameter(Mandatory = $false)][switch]$UseFiddlerProxy,
    # Optional token to specify.  Default is token obtained for current authentication context.
    [Parameter(Mandatory = $false)][SecureString]$token = $emptySecureString
  )

  Begin {
    $token = ValidateAndSetToken -token $token -tokenUri $KMUri
    AddSubstrateRouteHeaders -token $token
  }

  Process {
    if ($null -ne $_.$Name) {
      $Name = $_.$Name
    }

    if ($RunLocal.IsPresent) {
      $topicUri = $LocalUri
    }
    else {
      $topicUri = $KMUri
    }

    $topicUri += "/api/v1/feedbackDiagnostics/feedbackStatusByName?entityName=$Name&pipelineId=$PipelineId&region=$Region"

    if ($null -ne $ContextId) {
      $topicUri += "&contextId=$ContextId"
    }

    $headers["Client-Request-Id"] = [System.Guid]::NewGuid().ToString()
    $headers["SPHome-ClientType"] = "Diagnostics"

    try {
      if ($UseFiddlerProxy.IsPresent) {
        $data = Invoke-RestMethod -Method GET -Uri $topicUri -Authentication Bearer -Token $token -Headers $headers -Proxy $FiddlerProxy
      }
      else {
        $data = Invoke-RestMethod -Method GET -Uri $topicUri -Authentication Bearer -Token $token -Headers $headers
      }

      if ($null -ne $data) {
        $dataValue = $data.PSObject.Properties.Value
        if (0 -eq $dataValue) {
          return "No content found."
        }
        else {
          return $dataValue
        }
      }
    }
    catch {
      # throw
      $_
    }
  }
  End {}
}

function Set-TopicFeedback () {
  <#
    .SYNOPSIS
     Set Feedback
    .DESCRIPTION

    .EXAMPLE
     Set-TopicFeedback AL_hYazT2L8PDI844HtHQhRTQ Alexandria Confirmed
    #>
  [CmdletBinding()]
  param(
    # Id of topic.
    [Parameter(Mandatory = $true, ValueFromPipeline = $true)][String]$Id,
    # Name of topic.
    [Parameter(Mandatory = $true, ValueFromPipeline = $true)][String]$Name,
    # status to set the topic feedback to.
    [Parameter(Mandatory = $true, ValueFromPipeline = $true)][TopicFeedbackStatusType]$Status,
    # Pipeline Id
    [Parameter(Mandatory = $false)][byte]$PipelineId = 3,
    # Routes request to localhost.
    [Parameter(Mandatory = $false)][switch]$RunLocal,
    # use fiddler proxy.
    [Parameter(Mandatory = $false)][switch]$UseFiddlerProxy,
    # Optional token to specify.  Default is token obtained for current authentication context.
    [Parameter(Mandatory = $false)][SecureString]$token = $emptySecureString
  )

  Begin {
    $token = ValidateAndSetToken -token $token -tokenUri $KMUri
    AddSubstrateRouteHeaders -token $token
  }

  Process {
    if ($null -ne $_.Id) {
      $Id = $_.Id
    }

    if ($RunLocal.IsPresent) {
      $topicUri = $LocalUri
    }
    else {
      $topicUri = $KMUri
    }

    $topicUri += "/api/v1/feedbackDiagnostics/topicFeedbackStatus?entityId=$Id&entityName=$Name&feedbackStatus=$Status&pipelineId=$PipelineId"
    $headers["Client-Request-Id"] = [System.Guid]::NewGuid().ToString()
    $headers["SPHome-ClientType"] = "Diagnostics"

    try {
      if ($UseFiddlerProxy.IsPresent) {
        $data = Invoke-RestMethod -Method POST -Uri $topicUri -Authentication Bearer -Token $token -Headers $headers -Proxy $FiddlerProxy
      }
      else {
        $data = Invoke-RestMethod -Method POST -Uri $topicUri -Authentication Bearer -Token $token -Headers $headers
      }

      if ($null -ne $data) {
        return $data
      }
    }
    catch {
      # throw
      $_
    }
  }
  End {}
}

# Create a New Topic (a.k.a. Topic Lite)
function New-Topic () {
  <#
    .SYNOPSIS
     Create a New Topic (a.k.a. Topic Lite)
    .DESCRIPTION

    .EXAMPLE
     New-Topic -DisplayName "TopicName" -Description "Topic Description"
    #>
  [CmdletBinding()]
  param(
    # Topic display name
    [Parameter(Mandatory = $true)][ValidateLength(1, 1024)][String]$DisplayName,
    # Topic description
    [Parameter(Mandatory = $true)][String]$Description,
    # Client type
    [Parameter(Mandatory = $false)][ClientType]$ClientType = [ClientType]::Yammer,
    # Optional token to specify.  Default is token obtained for current authentication context.
    [Parameter(Mandatory = $false)][SecureString]$token = $emptySecureString,
    # Optional parameter to use the test pipeline
    [Parameter(Mandatory = $false)][Switch]$Test = $false
  )


  Begin {
    if ($token.Length -eq 0) {
      Write-Warning "Token is not passed so token will be obtained for current authentication context(request may route to MSIT)."
      $input = Read-Host "Enter 'Y' to proceed"
      if ($input -ne "Y") {
        throw 'Operation not allowed'
      }
    }
    $token = ValidateAndSetToken -token $token -tokenUri $SubstrateUri
    AddSubstrateRouteHeaders -token $token

    if ($Test) {
      $headers[$SPDFTestHeader] = "true"
    }
  }

  Process {
    if ($DisplayName -and $Description) {

      # Need to assemble payload
      $topicUri = "$SubstrateUri/KnowledgeGraph/api/v1.0/Topics/Create"
      # Build body
      $PostBody = "{ClientType: `"$ClientType`", TopicLite: {DisplayName: `"$DisplayName`", Definitions: [`"$Description`"]}}"

      # Create the new topic
      $data = $null
      $retry = 1

      while (($retry -ne 10) -and ($null -eq $data)) {
        try {
          $headers["Client-Request-Id"] = [System.Guid]::NewGuid().ToString()
          $data = Invoke-RestMethod -Method POST -Uri $topicUri -Authentication Bearer -Token $token -Headers $headers -Body $PostBody -ResponseHeadersVariable ResponseHeaders
        }
        catch {
          HandleRestError -Error $_ -Retry $retry -ResponseHeaders $ResponseHeaders
        }
      }
    }

    return $data.Id
  }
  End {
    Print-DebugInfo $ResponseHeaders
  }
}

$mockSiteId = "a479e569-b5bf-41ae-b61b-1348ec4a2bd6" # "$([System.Guid]::NewGuid().ToString())"
$mockWebId = "2925b6f1-45b6-4adb-8e7d-ae1bab39b0f7"
$mockUniqueId = "bb05ef2d-a65e-4c68-a727-307f1dbdc165"

function Get-TopicAnnotations () {
  <#
    .SYNOPSIS
    Retrieve annotations for text
    #>

  param(
    # Text or HTML to evaluate for topic annotations
    [Parameter(Mandatory = $false, ValueFromPipeline = $true)][String]$Text,
    # Text or HTML to evaluate for topic annotations
    [Parameter(Mandatory = $false)][String]$SourceId = "[No Source]",
    # Pull content of Windows Clipboard as content to evaluate for topic annotations
    [Parameter(Mandatory = $false)][Switch]$GetClipboard,
    # Get details of topics in addition to matches
    [Parameter(Mandatory = $false)][bool]$RetrieveTopicDetails = $true,
    # Secret switch
    [Parameter(Mandatory = $false)][Switch]$UseMicroservice,
    # If true, the HTTP calls will be cached.
    [Parameter(Mandatory = $false)][Switch]$EnableCache,
    # Additional header to add to the HTTP request
    [Parameter(Mandatory = $false)][Hashtable]$addHeaders = @{},
    # If present, no progress bar will be shown
    [Parameter(Mandatory = $false)][Switch]$Silent,
    # Topic quality threshold
    [Parameter(Mandatory = $false)][Int32]$Threshold = 1,
    # Optional token to specify.  Default is token obtained for current authentication context.
    [Parameter(Mandatory = $false)][SecureString]$token = $emptySecureString
  )

  Begin {
    $headersAnnotate = @{}

    if ($UseMicroservice) {
      $token = ValidateAndSetToken -token $token -tokenUri $KMUri
      $topicUri = "$KMUri/api/v2/knowledgebase/annotate?%24expand=EntityDetails"
      $headersAnnotate["SPHome-ClientType"] = "Diagnostics"
      $headersAnnotate["Accept"] = "*/*"
      $headersAnnotate["Content-Type"] = "application/json"

    }
    else {
      $token = ValidateAndSetToken -token $token -tokenUri $SubstrateUri
      AddSubstrateRouteHeaders -token $token
      $headersAnnotate["X-AnchorMailbox"] = $headers["X-AnchorMailbox"]
      $headersAnnotate["X-RoutingParameter-SessionKey"] = $headers["X-RoutingParameter-SessionKey"]
      $headersAnnotate["X-Debug-UseYukonHighlightApi"] = "true"
      $headersAnnotate["X-Debug-UseTenantTrie"] = "true"
      $contentType = "application/json; charset=UTF-8"
      $headersAnnotate["Accept"] = $contentType
      $headersAnnotate["Content-Type"] = $contentType
      $headersAnnotate["Sec-Fetch-Site"] = "cross-site"
      $headersAnnotate["Sec-Fetch-Mode"] = "cors"
      $headersAnnotate["Sec-Fetch-Dest"] = "empty"
      $headersAnnotate["X-ODataQuery"] = "true"
      $topicUri = "$SubstrateUri/KnowledgeGraph/api/v1.0/Annotation"
    }

    foreach ($key in $addHeaders.Keys) {
      $headersAnnotate[$key] = $addHeaders[$key]
    }

    $i = 0
  }

  Process {

    $headersAnnotate["Client-Request-Id"] = [System.Guid]::NewGuid().ToString()
    $headersAnnotate["X-Accept-Language"] = "MIXED"
    $headersAnnotate["X-Debug-FilterTopicLiteTopics"] = "false"

    $i++

    #JSON-encode string
    if ($GetClipboard) {
      $Text = Get-Clipboard -Raw
      Write-Verbose "Getting text from clipboard: $Text"
    }

    $jsonText = $Text | ConvertTo-Json -Compress -EscapeHandling:EscapeNonAscii

    if (!$Silent) {
      Write-Progress -Id 110 -Activity "Getting annotations" -Status "ID `"$sourceId`""
    }

    if (!$UseMicroservice) {
      # Build body, sloppy
      $PostBody = "{`"AnnotationRequest`":{`"Provider`":`"Yggdrasil`",`"TextSections`":[{`"Text`":$jsonText,`"SectionId`":`"[$sourceId]`",`"RetrieveTopicDetails`":true}]}}"
    }
    else {
      $PostBody = "{`"content`":[{`"ComponentId`":`"[$sourceId]`",`"Text`":$jsonText,`"PrePopulate`":true}],`"threshold`":$Threshold,`"siteId`":`"$mockSiteId`",`"webId`":`"$mockWebId`",`"uniqueId`":`"$mockUniqueId`"}"
    }

    if ($VerbosePreference -ne "SilentlyContinue") {
      Write-Verbose "HTTP request headers"
      foreach ($header in $headersAnnotate.Keys) { Write-Verbose "$header`: $($headersAnnotate[$header])" }
      Write-Verbose $PostBody
    }

    try {
      $response = Invoke-RestMethod-Cached -Method POST -Uri $topicUri -Authentication Bearer -Token $token -Headers $headersAnnotate -Body $PostBody -EnableCache:$EnableCache

      if ($VerbosePreference -ne "SilentlyContinue") {
        Write-Verbose $response | ConvertTo-Json
      }
      if ($response.EntityAnnotation) {
        return $response.EntityAnnotation
      }
      else {
        return $response.PageMatches
      }
    }
    catch {
      # throw
      $_
    }

  }
  End {
    if (!$Silent) {
      Write-Progress -Id 110 -Activity "f" -Completed
    }
  }
}

function Get-EvalTopicList() {
  <#
  .SYNOPSIS
  Retrieves Topic list via /api/v1.0/Topics/Evaluation/Mined?provider=Yggdrasil&pipeline=Dogfood&wholeTopicQualityModelVersion=V1. Default pipeline is Live, WTQ model V10
  .EXAMPLE
  Get-EvalTopicList -pipeline:Dogfood -wtqver:V11
  #>

  param(
    [Parameter(Mandatory = $false)][PipelineType]$pipeline = [PipelineType]::Live,
    [Parameter(Mandatory = $false)][WTQVersionType]$wtqVersion = [WTQVersionType]::V10,
    [Parameter(Mandatory = $false)][SecureString]$token
  )

  $topics = [System.Collections.ArrayList]::new()

  $token = ValidateAndSetToken -token $token -tokenUri $SubstrateUri
  AddSubstrateRouteHeaders -token $token

  # Declare internal KM API namespace for Managed Topics lists
  $topicUri = "$SubstrateUri/KnowledgeGraph/api/v1.0/Evaluation/Topics/Mined?provider=Yggdrasil&pipeline=" + $pipeline + "&wholeTopicQualityModelVersion=" + $wtqVersion

  try {
    $clientRequestId = [System.Guid]::NewGuid().ToString()
    Write-Host "Getting eval list from pipeline:`"$pipeline`" WTQ version:`"$wtqVersion`" Trace id $clientRequestId"

    $headers["Client-Request-Id"] = $clientRequestId
    $headers["Accept-Encoding"] = "gzip, deflate, br"
    $headers["X-DisableBufferOutput"] = "true"
    $headers["X-Accept-Language"] = "MIXED"
    $headers["X-Debug-FilterTopicLiteTopics"] = "false"

    Write-Host $topicUri
    $data = Invoke-RestMethod -Method GET -Uri $topicUri -Authentication Bearer -Token $token -Headers $headers -ResponseHeadersVariable responseHeaders
    Write-Host "Request-id: $($responseHeaders["request-id"])"
    # Check that we got some rows back
    if ($data.value.Length -ne 0) {
      Write-Debug "Records returned $($data.value.Length)"
      # Add them to the topics ArrayList
      [void]$topics.AddRange($data.value)

      $totalTopics = $data.'@odata.count'
    }
  }
  catch {
    $_
  }

  return $topics
}

function Get-EvalTopic() {
  <#
  .SYNOPSIS
  Retrieves Topic list via KnowledgeGraph/api/v1.0/Topics?id=AL_Y9Wssbz7Dlh9QpXtY7yZhw&provider=Yggdrasil&pipeline=Live&wholeTopicQualityModelVersion=V11
    . Default pipeline is Live, WTQ model V11
  .EXAMPLE
  Get-EvalTopic -ids:AL_Y9Wssbz7Dlh9QpXtY7yZhw -pipeline:Dogfood -wtqver:V11
  Get-EvalTopic -Id AL_02q7Qymt8EZo-YW9G56kkQ -pipeline Experimental_20 -GetRelatedTopics  -ResourceCount 4 -TopicGraphDepth 2 -Debug -Verbose
  #>

  param(
    [Parameter(Mandatory = $true)][ValidateLength(1, 255)][String]$Id,
    [Parameter(Mandatory = $false)][PipelineType]$pipeline = [PipelineType]::Live,
    [Parameter(Mandatory = $false)][WTQVersionType]$wtqVersion = [WTQVersionType]::V10,

    # If true, return related topics
    [Parameter(Mandatory = $false)][Switch]$GetRelatedTopics,
    # Maximum number of resources to return
    [Parameter(Mandatory = $false)][System.Int32]$ResourceCount = 2,
    # Topic graph depth
    [Parameter(Mandatory = $false)][System.Int32]$TopicGraphDepth = 1,

    [Parameter(Mandatory = $false)][SecureString]$token = $emptySecureString
  )

  Begin {
    $token = ValidateAndSetToken -token $token -tokenUri $SubstrateUri
    AddSubstrateRouteHeaders -token $token
  }

  Process {
    $topics = [System.Collections.ArrayList]::new()

    # Declare internal KM API namespace for Managed Topics lists
    $topicUri = "$SubstrateUri/KnowledgeGraph/api/v1.0/Evaluation/Topics?provider=Yggdrasil&Id=" + $Id + "&pipeline=" + $pipeline + "&wholeTopicQualityModelVersion=" + $wtqVersion

    try {
      $clientRequestId = [System.Guid]::NewGuid().ToString()
      Write-Host "Getting all topics... Trace id: $clientRequestId"
      $headers["Client-Request-Id"] = $clientRequestId
      $headers["Accept-Encoding"] = "gzip, deflate, br"
      $headers["X-DisableBufferOutput"] = "true"
      $headers["X-Accept-Language"] = "MIXED"
      $headers["X-Debug-FilterTopicLiteTopics"] = "false"

      # Add additional params if related topics are requested
      if ($GetRelatedTopics) {
        $topicUri += "&fileCount=$ResourceCount&peopleCount=$ResourceCount&siteCount=$ResourceCount&topicsGraphDepth=$TopicGraphDepth"
        $headers["X-Debug-EnableRelatedTopics"] = "true"
      }

      $data = Invoke-RestMethod -Method GET -Uri $topicUri -Authentication Bearer -Token $token -Headers $headers -ResponseHeadersVariable ResponseHeaders

      Write-Host "Request-id: $($responseHeaders["request-id"])"
      # Check that we got some rows back
      if ($data.value.Length -ne 0) {
        Write-Debug "Records returned $($data.value.Length)"

        # Add them to the topics ArrayList
        [void]$topics.AddRange($data.value)

        $totalTopics = $data.'@odata.count'
      }
    }
    catch {
      HandleRestError -Error $_ -Retry $retry
    }

    return $topics
  }
  End {
    Print-DebugInfo $ResponseHeaders
  }
}

##
function Download-Topics () {
  <#
  .SYNOPSIS
  Download Topics to a CSV file. The CSV file can then be used in a tool like Excel to analyze Topics and even bulk update them.
  By default, this command fetches the top 10 suggested topics sorted by quality score.
  .EXAMPLE
  Download-Topics
  #>

  [CmdletBinding()]
  param(
    # Topic type - Suggested, Confirmed, Published, Removed
    [Parameter(Mandatory = $false)][TopicListType]$topicListType = [TopicListType]::All,
    # Sort field - ConfirmedBy, DiscoveredDateTime, ImpressionCount, TopicName, TopicQualityScore
    [Parameter(Mandatory = $false)][SortFieldType]$sortField = [SortFieldType]::Quality,
    # Ascending or Descending
    [Parameter(Mandatory = $false)][SortDirectionType]$sortDirection = [SortDirectionType]::Descending,
    # Only return topics that start with this text
    [Parameter(Mandatory = $false)][string]$StartsWith = "",
    # Maximum number of topics to return
    [Parameter(Mandatory = $false)][System.Int32]$Count = 10,
    # pipeline id
    [Parameter(Mandatory = $false)][PipelineType]$pipeline = [PipelineType]::Live,
    # Only return topics that start with this text
    [Parameter(Mandatory = $false)][string]$csvPath = [String]"./topics.csv",
    # Only return the ID property of topics
    [Parameter(Mandatory = $false)][Switch]$IdOnly,
    # Optional token to specify.  Default is token obtained for current authentication context.
    [Parameter(Mandatory = $false)][SecureString]$token = $emptySecureString,
    # Determine whether we want topics to include or not include definitions
    [Parameter(Mandatory = $false)][DefinitionType]$DefinitionType = [DefinitionType]::All
  )

  # Check and bump
  $token = Get-UserToken -TokenToRenew:$token

  Write-Host "Getting top $Count topics from '$topicListType' list sorted by '$sortField' in '$sortDirection' order."
  $topics = Get-TopicList -TopicListType $topicListType -SortField $sortField -SortDirection $sortDirection -pipeline $pipeline -StartsWith $StartsWith -Count $Count -token $token

  $finalTopicList =  New-Object System.Collections.Generic.List[System.Object]
  if ($topics -ne $null) {
    $topicsLength = $topics.Length
    if ($topicsLength -gt $count) {
      $topicsLength = $count
    }
    
    $topicIds = ""
    for ($i = 0; $i -lt $topicsLength; $i++) {
      $alternativeNames = $topics[$i].AlternateNames -join ','
      Add-Member -InputObject $topics[$i] -Name "AlternativeNames" -MemberType NoteProperty -Value $alternativeNames
      Add-Member -InputObject $topics[$i] -Name "Status" -MemberType NoteProperty -Value $topics[$i].LifeCycleState
      Add-Member -InputObject $topics[$i] -Name "ModifiedByName" -MemberType NoteProperty -Value $topics[$i].ModifiedBy.DisplayName
      Add-Member -InputObject $topics[$i] -Name "TopicScore" -MemberType NoteProperty -Value $topics[$i].RelevanceScore
      Add-Member -InputObject $topics[$i] -Name "OrgTopicScore" -MemberType NoteProperty -Value $topics[$i].Quality
      
      if ($IdOnly -eq $false) {
        $topicIds = $topicIds -eq "" ? $topics[$i].Id : $topicIds + "," + $topics[$i].Id
        if ($i % 1000 -eq 999 -or $i -eq $topicsLength - 1)
        {
          $token = Get-UserToken -TokenToRenew:$token
            
          $percentComplete = $i * 100 / $topicsLength
          Write-Progress -Activity "Getting topics - $($topicIds)" -Status "$i of $topicsLength Complete" -PercentComplete $percentComplete
          
          $topicIdArray = $topicIds -split ","
          
          $topicsWithDefinitions = Get-Topic -Id $topicIdArray -token $token
            
          $count = $i % 1000 -ne 999 ? (($i % 1000) - 1) : 999
            
          for ($j = $count; $j -ge 0; $j--) {
            Add-Member -InputObject $topics[$i - $j] -Name "TopicType" -MemberType NoteProperty -Value $topicsWithDefinitions[$j].TopicType
            Add-Member -InputObject $topics[$i - $j] -Name "Definition" -MemberType NoteProperty -Value $topicsWithDefinitions[$j].Definition
          }
          
          $topicIds = ""
        }
      }
      $finalTopicList.Add($topics[$i])
    }

    $finalTopicList | select Id, Action, Name, AlternativeNames, Status, Definition, Created, Modified, ModifiedByName, TopicScore, OrgTopicScore, Impressions, Url | export-csv $csvPath
  }
  else {
    Write-Warning "No topics found"
  }
}

function Bulk-ConfirmRemove-Topics () {
  <#
  .SYNOPSIS
  Bulk confirm or remove topics from a CSV file. This command uses the Id and Action columns in the CSV file to confirm or reject topics. Valid values in the Id colum are "Confirm" OR "Remove"
  .EXAMPLE
  Bulk-ConfirmRemove-Topics -csvPath <path to csv file>
  #>

  [CmdletBinding()]
  param(
    # Only return topics that start with this text
    [Parameter(Mandatory = $true)][string]$csvPath,
    # Optional token to specify. Default is token obtained for current authentication context.
    [Parameter(Mandatory = $false)][SecureString]$token = $emptySecureString
  )

  $topics = Import-Csv $csvPath

  forEach ($topic in $topics) {
    if (
      (-not [string]::IsNullOrEmpty($topic.Id)) -and
      (-not [string]::IsNullOrEmpty($topic.Name)) -and
      ($topic.Action -eq "Confirm" -or $topic.Action -eq "Remove")
    ) {
      Write-Host "Processing topic [$($topic.Id)]. Action = [$($topic.Action)]."
      Set-Topic -Id $topic.Id -Action $topic.Action -token $token -autoApprove $true
      continue
    }

    # Error prompts
    if ([string]::IsNullOrEmpty($topic.Id)) {
      Write-Warning "Id cannot be empty. Ignoring topic [$($topic.Id)]."
    }

    if (-not [string]::IsNullOrEmpty($topic.Action) -and $topic.Action -ne "Confirm" -and $topic.Action -ne "Remove") {
      Write-Warning "Action has to be one of the following: Confirm, Remove. Specified Action '$($topic.Action)'. Ignoring topic [$($topic.Id)]."
    }

    if ([string]::IsNullOrEmpty($topic.Name)) {
      Write-Warning "Name cannot be empty. Ignoring topic [$($topic.Id)]."
    }
  }
}

#endregion

#region ################################################## Teams message commandlets ##################################################

function Get-TeamChannel () {
  <#
    .SYNOPSIS
     Get Team Channel metadata
    .DESCRIPTION

    .EXAMPLE

    #>
  [CmdletBinding()]
  param(
    [Parameter(Mandatory = $false)][String]$ChannelId,
    [Parameter(Mandatory = $false)][String]$TeamId,
    [Parameter(Mandatory = $false)][Uri]$Uri,
    [Parameter(Mandatory = $false)][SecureString]$token = $emptySecureString,
    [Parameter(Mandatory = $false)][Switch]$EnableCache
  )

  Begin {

    $token = ValidateAndSetToken -token $token -tokenUri $GraphUri

    if ($null -ne $Uri) {
      $parsedQueryString = [System.Web.HttpUtility]::ParseQueryString($Uri.Query)
      $TeamId = $parsedQueryString["groupId"]
      $ChannelId = $Uri.Segments[3]
    }

  }

  Process {

    if ($ChannelId -eq "") {
      $Uri = "$GraphUri/v1.0/Teams/$TeamId/channels"
    }
    else {

      if ($ChannelId -like "*%*") {
        # time to unescape
        $ChannelId = [System.Web.HttpUtility]::UrlDecode($ChannelId)
      }

      $Uri = "$GraphUri/v1.0/Teams/$TeamId/channels/$ChannelId"
    }

    try {
      $data = Invoke-RestMethod-Cached -Method GET -Uri $uri -Authentication Bearer -Token $token -Headers $headers -EnableCache:$EnableCache

      if ($null -eq $data.value) {
        $response = $data
      }
      else {
        $response = $data.value
      }

    }
    catch {
      $_
    }
    return $response

  }
  End {}
}

function Get-Team () {
  <#
    .SYNOPSIS

    .DESCRIPTION

    .EXAMPLE

    #>
  [CmdletBinding()]
  param(
    [Parameter(Mandatory = $false)][String]$id,
    [Parameter(Mandatory = $false)][SecureString]$token = $emptySecureString,
    [Parameter(Mandatory = $false)][Switch]$EnableCache
  )

  Begin {
    $token = ValidateAndSetToken -token $token -tokenUri $GraphUri
  }

  Process {

    if ($id -eq "") {
      $Uri = "$GraphUri/v1.0/me/joinedTeams"
    }
    else {
      $Uri = "$GraphUri/v1.0/Teams/$id"
    }

    try {
      $data = Invoke-RestMethod-Cached -Method GET -Uri $uri -Authentication Bearer -Token $token -Headers $headers -EnableCache:$EnableCache
      if ($null -eq $data.value) {
        $response = $data
      }
      else {
        $response = $data.value
      }

    }
    catch {
      $_
    }
    return $response

  }
  End {}
}

function Get-TeamMessage () {
  <#
    .SYNOPSIS
    Retrieves messages from a Team channel
    .DESCRIPTION

    .EXAMPLE

    #>
  [CmdletBinding()]
  param(
    [Parameter(Mandatory = $false)][String]$Id,
    [Parameter(Mandatory = $false)][String]$ChannelId,
    [Parameter(Mandatory = $false)][String]$TeamId,
    [Parameter(Mandatory = $false)][Uri]$Uri,
    [Parameter(Mandatory = $false)][Int32]$Top = 100,
    [Parameter(Mandatory = $false)][Switch]$Chats,
    [Parameter(Mandatory = $false)][SecureString]$token = $emptySecureString,
    [Parameter(Mandatory = $false)][Switch]$EnableCache,
    [Parameter(Mandatory = $false)][Switch]$Silent
  )

  Begin {
    $token = ValidateAndSetToken -token $token -tokenUri $GraphUri

    if ($null -ne $Uri) {
      $parsedQueryString = [System.Web.HttpUtility]::ParseQueryString($Uri.Query)
      $TeamId = $parsedQueryString["groupId"]
      $ChannelId = $Uri.Segments[3]
      if ($Uri.Segments[2] -ieq "message/") {
        # a message URL was passed
        $Id = $Uri.Segments[4]
      }
    }

    # Breakdown token to get appropriate value for Substrate anchor for KM API calls
    $parsedToken = Parse-JWTtoken($token)

    $messages = [System.Collections.ArrayList]::new()
  }

  Process {

    if ($ChannelId -like "*%*") {
      # time to unescape
      $ChannelId = [System.Web.HttpUtility]::UrlDecode($ChannelId)
    }

    if ($Chats) {
      $Uri = "$GraphUri/beta/users/$($parsedToken.upn)/chats/getAllMessages?top=$top"
    }
    elseif ($id -eq "") {
      $Uri = "$GraphUri/beta/Teams/$TeamId/channels/$ChannelId/messages"
    }
    else {
      $Uri = "$GraphUri/beta/Teams/$TeamId/channels/$ChannelId/messages/$id"
    }

    $page = 1
    $totalTimer = [System.Diagnostics.Stopwatch]::StartNew()

    while (($page -eq 1) -or ($data.'@odata.nextLink')) {

      # Reset
      $data = $null

      if (!$Silent) {
        Write-Progress -Id 101 -Activity "Fetching messages from $Uri" -Status "In progress, page $page. $($messages.Count) messages retrieved. Elapsed time: $([Math]::Floor($totalTimer.ElapsedMilliseconds / 1000)) seconds."
      }

      $retry = 1

      while (($retry -ne 10) -and ($null -eq $data)) {
        try {
          $data = Invoke-RestMethod-Cached -Method GET -Uri $Uri -Authentication Bearer -Token $token -Headers $headers -EnableCache:$EnableCache
        }
        catch {
          HandleRestError -Error $_ -Retry $retry
        }
      }


      if ($null -eq $data.value) {
        # There's just one record back
        [void]$messages.Add($data)
      }
      else {
        [void]$messages.AddRange($data.value)
      }

      if ($null -ne $data.'@odata.nextLink') {
        $Uri = $data.'@odata.nextLink'
      }

      $page++

      if ($messages.Length.Length -ge $Top) {
        break
      }
    }

    if (!$Silent) {
      Write-Progress -Id 101 -Activity "Done" -Completed
    }

    # Now get replies
    $token = ValidateAndSetToken -token $token -tokenUri $GraphUri

    $replies = [System.Collections.ArrayList]::new()
    $i = 1


    foreach ($message in $messages) {

      if ($Chats) {
        $Uri = "$GraphUri/beta/me/chats/replies"
      }
      else {
        $Uri = "$GraphUri/beta/Teams/$TeamId/channels/$ChannelId/messages/$($message.id)/replies"
      }

      $page = 1

      while (($page -eq 1) -or ($data.'@odata.nextLink')) {

        $sumCount = $messages.Count + $replies.Count

        if ($messages.Subject -eq "") {
          $subject = "[No Subject]"
        }
        else {
          $subject = $messages.Subject
        }

        if (!$Silent) {
          Write-Progress -Id 101 -Activity "Fetching replies to message ID $($message.id), `"$subject`", page $page" -Status "In progress. $sumCount messages retrieved. Elapsed time: $([Math]::Floor($totalTimer.ElapsedMilliseconds / 1000)) seconds." -PercentComplete:(($i / $messages.Count) * 100)
        }
        $data = $null
        $retry = 1

        while (($retry -ne 10) -and ($null -eq $data)) {
          try {
            $data = Invoke-RestMethod-Cached -Method GET -Uri $Uri -Authentication Bearer -Token $token -Headers $headers -EnableCache:$EnableCache
            if (!$Silent) {
              Write-Progress -Id 101 -Activity "Fetching messages from $Uri" -Status "In progress, page $page. $($messages.Count) messages retrieved. ($($_.Exception.Response.StatusCode)) Elapsed time: $([Math]::Floor($totalTimer.ElapsedMilliseconds / 1000)) seconds."
            }
          }
          catch {
            HandleRestError -Error $_ -Retry $retry
          }
        }

        if ($null -eq $data.value) {
          [void]$replies.Add($data)
        }
        else {
          [void]$replies.AddRange($data.value)
        }

        if ($messages.Length.Length + $replies.Length.Length -ge $Top) {
          break
        }

        if ($null -ne $data.'@odata.nextLink') {
          $Uri = $data.'@odata.nextLink'
        }

        $page++
      }

      if ($messages.Length.Length + $replies.Length.Length -ge $Top) {
        break
      }

      $i++
    }

    if ($replies.Count -gt 0) {
      [void]$messages.AddRange($replies)
    }

    return $messages
  }

  End {
    Write-Progress -Id 101 -Activity "Done" -Completed
  }
}
#endregion
#region ################################################## Substrate message commandlets ##################################################

function Get-SubstrateMessage () {
  <#
    .SYNOPSIS
     Retrieves messages in conversation by app, user/group, channel/folder or message ID
    .DESCRIPTION

    .EXAMPLE

    #>
  [CmdletBinding()]
  param(
    [Parameter(Mandatory = $false)][Int32]$PageSize = 100,
    [Parameter(Mandatory = $false)][Int32]$MaxCount = [Int32]::MaxValue,
    [Parameter(Mandatory = $false)][String]$PartitionUpn = "",
    [Parameter(Mandatory = $false)][String[]]$Select,
    [Parameter(Mandatory = $false)][Switch]$FullMessage = $false,
    [Parameter(Mandatory = $false)][Switch]$PreferText = $false,
    [Parameter(Mandatory = $false)][Switch]$FocusedInbox = $false,
    [Parameter(Mandatory = $false)][ConversationType]$Workload = [ConversationType]::Teams,
    [Parameter(Mandatory = $false)][SecureString]$token = $emptySecureString,
    [Parameter(Mandatory = $false)][Hashtable]$addHeaders = @{},
    [Parameter(Mandatory = $false)][Switch]$EnableCache
  )

  Begin {
    $token = ValidateAndSetToken -token $token -tokenUri $GraphUri
    AddSubstrateRouteHeaders -token $token

    # Breakdown token to get appropriate value for Substrate anchor for KM API calls
    $parsedToken = Parse-JWTtoken($token)

    if ($PartitionUpn -eq "") {
      $PartitionUpn = $null -ne $parsedToken.upn ? $parsedToken.upn : $parsedToken.smtp # Get UPN, failing that, SMTP
      $shardtype = "users"
    }
    else {
      # TODO: is it a user or group?
      $data = Get-PersonMetadata $PartitionUpn
      $shardtype = $data.'@odata.context' -eq ("https://graph.microsoft.com/v1.0/`$metadata#groups") ? "groups" : "users"
      $shardId = $data.value[0].id
      #$totalTopics = $null -ne $latestSnapshot.Value ? $latestSnapshot.Value : $topicsPerPage
    }
    # Get unfiltered HTML body, and ask for IDs that don't change
    $headers["Prefer"] = "outlook.allow-unsafe-html"

    if ($PreferText) {
      $headers["Prefer"] = "outlook.body-content-type=`"text`""
    }

    foreach ($key in $addHeaders.Keys) {
      $headers[$key] = $addHeaders[$key]
    }

    $messages = [System.Collections.ArrayList]::new()
  }

  Process {

    $Uri = "$GraphUri/beta/$shardtype/$PartitionUpn/"

    switch ($Workload) {
      "Teams" {
        $Uri += "MailFolders/TeamsMessagesData/";
        Break
      }
      "Yammer" {
        $Uri += "MailFolders/Yammer/";
        Break
      }
      "Outlook" {
        $Uri += "MailFolders/Inbox/"
        Break
      }
      Default {
        Write-Error
      }
    }
    # Get folder metadata - count of messages
    $data = $null
    $retry = 1
    while (($retry -ne 10) -and ($null -eq $data)) {
      try {
        $data = Invoke-RestMethod-Cached -Method GET -Uri $Uri -Authentication Bearer -Token $token -Headers $headers -EnableCache:$EnableCache
      }
      catch {
        if (($_.Exception.Response.StatusCode -eq 429 ) -or ($_.Exception.Response.StatusCode -eq 502) -or $_.ToString() -contains "ApplicationThrottled") {
          # We're getting throttled or getting one of those spurious Bad Gateway responses; back off and try again
          $backoff = $retry * 15
          $retry++
          Write-Progress -Id 102 -Activity "Retry $retry. Sleeping $backoff seconds..." -ParentId 101
          Write-Host "Sleeping $backoff seconds..."
          Start-Sleep -seconds $backoff
          Write-Progress -Id 102 -Activity "Retry $retry. Sleeping $backoff seconds..." -ParentId 101 -Completed
        }
        else {
          Write-Host $_.Exception.Message
          Write-Host $_.Exception.StackTrace
          throw $_
        }
      }
    }

    $count = [math]::Min($data.totalItemCount, $MaxCount)

    if ($Select) {
      $selected = ""
      foreach ($field in $Select) {
        if ($selected.Length -gt 0) { $selected += "," }
        $selected += $field
      }
      $Uri += "messages/?top=$PageSize&select=$selected"
    }
    else {
      if ($FullMessage) {
        $Uri += "messages/?&top=$PageSize"
      }
      else {
        $Uri += "messages/?select=Body,InternetMessageId,ReceivedDateTime,From,ToRecipients,Subject,ChangeKey&top=$PageSize"
      }
    }

    if ($FocusedInbox) {
      $Uri += "&`$orderby=InferenceClassification, createdDateTime DESC&filter=InferenceClassification ne 'Other'"
    }

    $page = 1
    $totalTimer = [System.Diagnostics.Stopwatch]::StartNew()

    while ((($page -eq 1) -or ($data.'@odata.nextLink')) -and ($messages.Count -lt $count)) {

      # Reset
      $data = $null

      $retry = 1

      while (($retry -ne 10) -and ($null -eq $data)) {

        $token = ValidateAndSetToken -token $token -tokenUri $SubstrateUri

        if ($messages.Count -gt 0) {
          $timePerMessage = ($totalTimer.ElapsedMilliseconds / $messages.Count ) / 1000
          $messagesLeft = $count - $messages.Count
          $timeleft = [Math]::Floor($messagesLeft * $timePerMessage)
        }

        else {
          $timePerMessage = 0
          $timeleft = 0
        }

        Write-Progress -Id 101 -Activity "Fetching messages from $Uri" -Status "In progress, page $page. $($messages.Count) messages retrieved of $count. Elapsed time: $([Math]::Floor($totalTimer.ElapsedMilliseconds / 1000)) seconds." -PercentComplete (($messages.Count / ($count + 1)) * 100) -SecondsRemaining $timeleft

        if ($VerbosePreference -ne "SilentlyContinue") {
          Write-Verbose "HTTP request headers"
          foreach ($header in $headers.Keys) { Write-Verbose "$header`: $($headers[$header])" }
        }

        try {
          $data = Invoke-RestMethod-Cached -Method GET -Uri $Uri -Authentication Bearer -Token $token -Headers $headers -EnableCache:$EnableCache
        }
        catch {
          HandleRestError -Error $_ -Retry $retry
        }
      }


      if ($null -eq $data.value) {
        # There's just one record back
        [void]$messages.Add($data)
      }
      else {
        [void]$messages.AddRange($data.value)
      }

      if ($null -ne $data.'@odata.nextLink') {
        $Uri = $data.'@odata.nextLink'
      }

      $page++
    }

    return $messages
  }
  End {
    Write-Progress -Id 101 -Activity "Done" -Completed
  }
}

#endregion
#region ################################################## Person data commandlets ##################################################

function Get-PersonMetadata () {
  <#
    .SYNOPSIS
    Returns the metadata for the UPN passed
    #>
  param(
    # User Principal Name
    [Parameter(Mandatory = $false)][string]$Upn,
    # Set properties to return; default is to get everything
    [Parameter(Mandatory = $false)][string[]]$selectProperties = $null,
    # Optional token to specify.  Default is token obtained for current authentication context.
    [Parameter(Mandatory = $false)][SecureString]$token = $emptySecureString
  )

  if ("" -eq $Upn) {
    if ($token.Length -eq 0) {
      # If UPN isn't passed without a token, a shortcut to get current user UPN without off-box calls
      $Upn = Invoke-Expression 'whoami /upn'
      if ($null -eq $Upn) {
        Write-Error "No UPN passed as parameter or available from current user." -ErrorAction:Stop
      }
      Write-Verbose "No user specified, using UPN for current user $Upn"
    }
    else {
      # Get the UP from the passed token
      $parsedToken = Parse-JWTtoken ($token)
      $Upn = $null -ne $parsedToken.Upn ? $parsedToken.Upn : $parsedToken.smtp  #Token UPN - fall back to SMTP attribute if UPN isn't present
    }
  }

  $token = ValidateAndSetToken -token $token -tokenUri $GraphUri

  if ($null -eq $selectProperties) {
    $select = ""
  }
  else {
    # Assemble query string
    $select = "`?$select="

    $i = 1
    foreach ($property in $selectProperties) {
      $select += $property
      if ($i -ne $selectProperties.Length) {
        $select += ","
      }
      $i++
    }
  }

  $uri = "https://graph.microsoft.com/beta/users(`'$Upn`')$select"

  try {
    $data = Invoke-RestMethod -Method GET -Uri $uri -Authentication Bearer -Token $token -Headers $headers
    $response = $data
  }
  catch {
    try {
      if ($_.Exception.Response.StatusCode -eq 404) {
        # Groups next - extreme laziness
        $uri = "https://graph.microsoft.com/v1.0/groups/?`$filter=(mail+eq+`'$Upn`')$select"
        $data = Invoke-RestMethod -Method GET -Uri $uri -Authentication Bearer -Token $token -Headers $headers
        $response = $data
      }
      else {
        Write-Error $_.Exception.Response -ErrorAction:Stop
      }
    }
    catch {
      Write-Error $_.Exception.Response -ErrorAction:Stop
    }
  }
  return $response
}


function Get-AzureAdGroupMembership () {
  <#
    .SYNOPSIS
    Returns list of members for group UPN passed
    .EXAMPLE
    Get-AzureAdGroupMembership vivatopicsdogfood@microsoft.com | where {$_.'@odata.type' -ne "#microsoft.graph.group"} | ft mail
    #>
  param(
    # User Principal Name
    [Parameter(Mandatory = $false)][string]$Upn,
    # Optional token to specify.  Default is token obtained for current authentication context.
    [Parameter(Mandatory = $false)][SecureString]$token = $emptySecureString
  )

  if ("" -eq $Upn) {
    Write-Error "No UPN passed as parameter." -ErrorAction:Stop
  }

  $token = ValidateAndSetToken -token $token -tokenUri $GraphUri

  # First, find AAD group ID from the UPN
  try {
    $uri = "https://graph.microsoft.com/v1.0/groups/?`$filter=(mail+eq+`'$Upn`')"
    $data = Invoke-RestMethod -Method GET -Uri $uri -Authentication Bearer -Token $token -Headers $headers
    if ($data.value.Length -eq 1) {
      $Id = $data.value[0].id
    }
    else {
      Write-Error "More than one match for query" -ErrorAction:Stop
    }
  }
  catch {
    Write-Error $_.Exception -ErrorAction:Stop
  }

  # Now, start fetching membership.  It might be paged if membership is greater than X (currently 100)
  $uri = "https://graph.microsoft.com/v1.0/groups/{$Id}/transitiveMembers"

  $page = 1
  $response = [System.Collections.ArrayList]::new()
  $data = $null

  while (($page -eq 1) -or ($data.'@odata.nextLink')) {
    try {
      $data = Invoke-RestMethod -Method GET -Uri $uri -Authentication Bearer -Token $token -Headers $headers

      # Will be full object(s)
      [void]$response.AddRange($data.value)
      $page++
    }
    catch {
      Write-Error $_.Exception -ErrorAction:Stop
    }

    if ($data.'@odata.nextLink') {
      $uri = $data.'@odata.nextLink'
    }
  }
  return $response
}


#endregion
