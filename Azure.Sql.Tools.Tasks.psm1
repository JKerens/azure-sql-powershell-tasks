using module Az.Sql
using module SqlServer
using namespace System.Management.Automation

class ValidQueries : IValidateSetValuesGenerator {
    [string[]] GetValidValues() {   
        return (_getQueries).Name
    }
}

<#
.DESCRIPTION
Multi-threaded Queries that add 1 job per elastic pool

.EXAMPLE
$databases = Get-AzSqlDatabase -ResourceGroupName MyRG -ServerName MyServerName
$databases | Invoke-AzureSqlCmd -QueryName GetDatabaseName.sql
#>
function Invoke-AzureSqlCmd {
    [CmdletBinding()]
    param (
        [Parameter(Mandatory = $true, Position = 0, ValueFromPipeline = $true)]
        [ValidateNotNullOrEmpty()]
        [Microsoft.Azure.Commands.Sql.Database.Model.AzureSqlDatabaseModel]$Database,

        [Parameter(Mandatory = $true, Position = 1)]
        [ValidateSet([ValidQueries])]
        [string]$QueryName,

        [Parameter(Mandatory = $false, Position = 2)]
        [int]$ThrottleLimit = 10
    )
    # this reads the file and demands you provide the parameters required to execute
    # ref - https://docs.microsoft.com/en-us/powershell/module/microsoft.powershell.core/about/about_functions_advanced_parameters?view=powershell-7.1#dynamic-parameters
    DynamicParam {
        $paramDictionary = _getQueryDynamicParameters -QueryName $QueryName
        return $paramDictionary
    }
    begin {
        # retrieves the query file
        $queryFile = _getQueries | Where-Object { $_.Name -eq $QueryName }
        $accessToken = (Get-AzAccessToken -ResourceUrl "https://database.windows.net").Token
    }
    process {
        if (-not $dataset) {
            $dataset = _addDatabaseJob -QueryFile $QueryFile -AccessToken $AccessToken -Database $Database
        }
        else {
            $dataset = _addDatabaseJob -Jobs $dataset -QueryFile $QueryFile -AccessToken $AccessToken -Database $Database
        }
    }
    end {
        return _runProgressJob -Dataset $dataset
    } 
}

<#
.DESCRIPTION 
Creates the required tables for tracking multiple sync jobs
#>
function _runProgressJob($Dataset) {
        # Create a hashtable for process.
        # Keys should be ID's of the processes
        $origin = @{}
        $Dataset | Foreach-Object { $origin.($_.id) = @{} }
        
        # Create synced hashtable
        $sync = [System.Collections.Hashtable]::Synchronized($origin)
        
        # Run Jobs
        $job = $Dataset | Foreach-Object -ThrottleLimit $ThrottleLimit -AsJob -Parallel {
            $syncCopy = $using:sync
            $process = $syncCopy.$($PSItem.Id)
        
            $process.Id = $PSItem.Id
            # Formatting allows clean progress bar width
            $process.Activity = ("{0,-20}" -f $PSItem.ElasticPoolName).Substring(0,20)
            $process.Status = "Starting"
            $process.PercentComplete = 0

            # Process. update activity
            $counter = 0
            foreach ($sqlCmd in $PSItem.Sqlcmds) {
                # Update process on status
                $process.Status = "$($sqlCmd.Database)"
                $process.PercentComplete = (($counter / $PSItem.Sqlcmds.Count) * 100)
                
                $retries = 0
                do {                  
                    try {
                        $result = [PSCustomObject]@{
                            Database = $sqlCmd.Database
                            Result   = Invoke-Sqlcmd @sqlCmd
                        }
                        $result | Write-Output
                        break;
                    }
                    catch {
                        Write-Error "$($sqlCmd.Database) - $_"
                        $retries++
                        # Try refreshing the token just in case
                        $sqlCmd.AccessToken = (Get-AzAccessToken -ResourceUrl "https://database.windows.net").Token
                        # Softly back off more per retry
                        Start-Sleep -Seconds ([timespan]::FromSeconds([Math]::Pow(2,$retries))).TotalSeconds
                    }    
                } while ($retries -lt $RetryCount)
                $counter++
            }
            # Mark process as completed
            $process.Completed = $true
        }
        return (_awaitJob -Job $job)
}

<#
.DESCRIPTION
Basically a PowerShell version of .WhenAll() blended with a sync UI update hook
#>
function _awaitJob {
    [CmdletBinding()]
    param (
        [Parameter(Mandatory)]
        [System.Management.Automation.PSTasks.PSTaskJob]$Job
    )
    # Get-Progress
    while ($job.State -eq 'Running') {
        $sync.Keys | Foreach-Object {
            # If key is not defined, ignore
            if (![string]::IsNullOrWhiteSpace($sync.$_.keys)) {
                # Create parameter hashtable to splat
                $param = $sync.$_
    
                # Execute Write-Progress
                Write-Progress @param
            }
        }
    
        # Wait to refresh to not overload gui
        Start-Sleep -Seconds 0.1
    } 
    return (Receive-Job -Job $job)
}

function _getQueryDynamicParameters {
    [CmdletBinding()]
    param (
        [Parameter(Mandatory)]
        [string]$QueryName
    )
    $queryFile = _getQueries | Where-Object { $_.Name -eq $QueryName }
    $queryParameters = _getQueryParameters -QueryFile $queryFile

    if ($null -ne $queryParameters) {
        $attributes = New-Object -Type System.Management.Automation.ParameterAttribute
        $attributes.ParameterSetName = "QueryParameters"
        $attributes.Mandatory = $true
        $attributeCollection = New-Object -Type System.Collections.ObjectModel.Collection[System.Attribute]
        $attributeCollection.Add($attributes)
        $paramDictionary = New-Object -Type System.Management.Automation.RuntimeDefinedParameterDictionary

        foreach ($queryParameter in $queryParameters) {
            $dynParam = New-Object -Type System.Management.Automation.RuntimeDefinedParameter($queryParameter, [string], $attributeCollection)
            $paramDictionary.Add($queryParameter, $dynParam)
        }
  
        return $paramDictionary
    }
}

# returns all query files [System.IO.FileInfo[]]
function _getQueries {
    return Get-ChildItem "$PSScriptRoot\Queries\*.sql"
}

# this finds all the parameters in your sql files that are Invoke-SqlCmd friendly
function _getQueryParameters {
    [CmdletBinding()]
    param (
        [Parameter(Mandatory)]
        [System.IO.FileInfo]$QueryFile
    )
    $group = "parameters"
    $result = Get-Content $QueryFile.FullName `
    | Select-String "\'\$\((?<$group>(\w+))\)\'"

    return ($result.Matches.Groups | Where-Object { $_.Name -eq $group }).Value
}

<#
.DESCRIPTION
TaskFactory to add a job to the list
#>
function _addDatabaseJob {
    [OutputType([System.Collections.Hashtable])]
    [CmdletBinding()]
    param (
        [Parameter(Mandatory = $false)]
        [ValidateNotNullOrEmpty()]
        [System.Object[]]$Jobs = @(),

        [Parameter(Mandatory)]
        [System.IO.FileInfo]$QueryFile,

        [Parameter(Mandatory)]
        [ValidateNotNullOrEmpty()]
        [string]$AccessToken,

        [Parameter(Mandatory)]
        [ValidateScript({ 
                $_.GetType() -eq [Microsoft.Azure.Commands.Sql.Database.Model.AzureSqlDatabaseModel] `
                    -or # this allows for unit testing
                ($_ | Get-Member).TypeName -eq "Deserialized.Microsoft.Azure.Commands.Sql.Database.Model.AzureSqlDatabaseModel"
            })]
        [PSCustomObject]$Database,

        [Parameter(Mandatory = $false)]
        [System.Management.Automation.RuntimeDefinedParameter]$ParamDictionary
    )
    <# Basic SqlCmd splat #>
    $invokeSqlCmdParams = @{
        ServerInstance = "$($Database.ServerName).database.windows.net"
        Database       = $Database.DatabaseName
        InputFile      = $QueryFile
        AccessToken    = $AccessToken
    }

    <# $group is the current item #>
    $ElasticPoolName = $Database.ElasticPoolName ?? "NA"

    <# 
        index can be null on the first job so set to 1 
        Sort-Object should grab the last Id so 1,2 vs 2,1 order will both return 2
    #>
    $index = ($Jobs | Sort-Object -Property Id | Select-Object -Last 1).Id ?? 0
        
    # only adds the variable argument to the splat if there are arguments
    if ($null -ne $ParamDictionary.Values) {
        [string[]]$queryParametersValues = $ParamDictionary.Values | ForEach-Object { "$($_.Name)=$($_.Value)" }
        $invokeSqlCmdParams.Add("Variable", $queryParametersValues)
    }

    if ($null -ne ($Jobs | Where-Object { $_.ElasticPoolName -eq $ElasticPoolName })) {
        <# Add to existing ElasticPool Job #>
        ($Jobs | Where-Object { $_.ElasticPoolName -eq $ElasticPoolName }).SqlCmds += $invokeSqlCmdParams
    }
    else {
        <# Create new ElasticPool Job entry #>
        $runParameters = @{
            Id              = ++$index
            ServerInstance  = $invokeSqlCmdParams.ServerInstance
            ElasticPoolName = $ElasticPoolName
            SqlCmds         = @()
        }
        $runParameters.SqlCmds += $invokeSqlCmdParams
        $Jobs += $runParameters
    }
    
    return $Jobs
}