*** Settings ***
Documentation       The Library relies on the following variables being available on execution
...                 SDCTL: Pointing to the client binary
...                 DATA-SERVER-IP: IP of data server
...                 DATA-SERVER-PORT: TCP port of data server
...                 SCHEMA-SERVER-IP: IP of schema server
...                 SCHEMA-SERVER-PORT: TCP port of schema server

Library             String
Library             Process
Library             OperatingSystem
Library             Collections


*** Keywords ***
ListDataStores
    [Documentation]    List datastores in a data-server
    ${result} =    Run Process
    ...    ${SDCTL}
    ...    -a    ${DATA-SERVER-IP}:${DATA-SERVER-PORT}
    ...    datastore
    ...    list
    Log    ${result.rc}
    Log    ${result.stdout}
    Log    ${result.stderr}
    RETURN    ${result}

GetDataStore
    [Documentation]    Get a target from a data-server
    [Arguments]    ${datastore}
    ${result} =    Run Process
    ...    ${SDCTL}
    ...    -a    ${DATA-SERVER-IP}:${DATA-SERVER-PORT}
    ...    datastore
    ...    get
    ...    --ds
    ...    ${datastore}
    Log    ${result.rc}
    Log    ${result.stdout}
    Log    ${result.stderr}
    RETURN    ${result}

TransactionSet
    [Documentation]    Start an intent transaction
    [Arguments]
    ...    ${datastore}
    ...    ${transactionId}
    ...    @{intents}
    
    ${intentargs} =    Create List

    FOR    ${file}    IN    @{intents}
        Append To List    ${intentargs}    -i    ${file}
    END

    ${result} =    Run Process
    ...    ${SDCTL}
    ...    -a    ${DATA-SERVER-IP}:${DATA-SERVER-PORT}
    ...    data
    ...    transaction
    ...    set
    ...    --ds
    ...    ${datastore}
    ...    --transaction-id
    ...    ${transactionId}
    ...    @{intentargs}

    Log    ${result.rc}
    Log    ${result.stdout}
    Log    ${result.stderr}
    RETURN    ${result}

TransactionConfirm
    [Documentation]    Confirm an ongoing transaction
    [Arguments]
    ...    ${datastore}
    ...    ${transactionId}
    ${result} =    Run Process
    ...    ${SDCTL}
    ...    -a    ${DATA-SERVER-IP}:${DATA-SERVER-PORT}
    ...    data
    ...    transaction
    ...    confirm
    ...    --ds
    ...    ${datastore}
    ...    --transaction-id 
    ...    ${transactionId}
    ...    
    Log    ${result.rc}
    Log    ${result.stdout}
    Log    ${result.stderr}
    RETURN    ${result}
TransactionCancel
    [Documentation]    Cancel (Roleback) an ongoing transaction
    [Arguments]
    ...    ${datastore}
    ...    ${transactionId}
    ${result} =    Run Process
    ...    ${SDCTL}
    ...    -a    ${DATA-SERVER-IP}:${DATA-SERVER-PORT}
    ...    data
    ...    transaction
    ...    cancel
    ...    --ds
    ...    ${datastore}
    ...    --transaction-id 
    ...    ${transactionId}
    ...    
    Log    ${result.rc}
    Log    ${result.stdout}
    Log    ${result.stderr}
    RETURN    ${result}
CreateDataStore
    [Documentation]    Create a target in a data-server
    [Arguments]
    ...    ${datastore}
    ...    ${target-definition-file}
    ...    ${target-sync-file}
    ...    ${schema-name}
    ...    ${schema-vendor}
    ...    ${schema-version}
    ${result} =    Run Process
    ...    ${SDCTL}
    ...    -a    ${DATA-SERVER-IP}:${DATA-SERVER-PORT}
    ...    datastore
    ...    create
    ...    --ds
    ...    ${datastore}
    ...    --target
    ...    ${target-definition-file}
    ...    --sync
    ...    ${target-sync-file}
    ...    --name
    ...    ${schema-name}
    ...    --vendor
    ...    ${schema-vendor}
    ...    --version
    ...    ${schema-version}

    Log    ${result.rc}
    Log    ${result.stdout}
    Log    ${result.stderr}
    RETURN    ${result}

DeleteDatastore
    [Documentation]    Delete a target from a data-server
    [Arguments]    ${datastore}
    ${result} =    Run Process
    ...    ${SDCTL}
    ...    -a    ${DATA-SERVER-IP}:${DATA-SERVER-PORT}
    ...    datastore
    ...    delete
    ...    --ds
    ...    ${datastore}

    Log    ${result.rc}
    Log    ${result.stdout}
    Log    ${result.stderr}
    RETURN    ${result}

CreateCandidate
    [Documentation]    Create a new named Candidate in the given Datastore.
    [Arguments]    ${datastore}    ${candidate}    ${owner}    ${priority}
    ${result} =    Run Process
    ...    ${SDCTL}
    ...    -a
    ...    ${DATA-SERVER-IP}:${DATA-SERVER-PORT}
    ...    datastore
    ...    create
    ...    --ds
    ...    ${datastore}
    ...    --candidate
    ...    ${candidate}
    ...    --owner
    ...    ${owner}
    ...    --priority
    ...    ${priority}

    Log    ${result.rc}
    Log    ${result.stdout}
    Log    ${result.stderr}
    RETURN    ${result}

GetCandidate
    [Documentation]    Retrieve the given candidate from the referenced Datastore
    [Arguments]    ${datastore}    ${candidate}
    ${result} =    Run Process
    ...    ${SDCTL}
    ...    -a    ${DATA-SERVER-IP}:${DATA-SERVER-PORT}
    ...    datastore    get
    ...    --ds    ${datastore}
    ...    --candidate    ${candidate}

    Log    ${result.rc}
    Log    ${result.stdout}
    Log    ${result.stderr}
    RETURN    ${result}

DeleteCandidate
    [Documentation]    Delete a named Candidate in the given Datastore
    [Arguments]    ${datastore}    ${candidate}
    ${result} =    Run Process
    ...    ${SDCTL}
    ...    -a    ${DATA-SERVER-IP}:${DATA-SERVER-PORT}
    ...    datastore
    ...    delete
    ...    --ds
    ...    ${datastore}
    ...    --candidate
    ...    ${candidate}

    Log    ${result.rc}
    Log    ${result.stdout}
    Log    ${result.stderr}
    RETURN    ${result}

Diff
    [Documentation]    Performs a diff on a datastore and a candidate
    [Arguments]    ${datastore}    ${candidate}
    ${result} =    Run Process
    ...    ${SDCTL}
    ...    -a    ${DATA-SERVER-IP}:${DATA-SERVER-PORT}
    ...    data    diff
    ...    --ds    ${datastore}
    ...    --candidate    ${candidate}

    Log    ${result.rc}
    Log    ${result.stdout}
    Log    ${result.stderr}
    RETURN    ${result}

Commit
    [Documentation]    Performs a commit on the given datastore/candidate and returns the Process Result object https://robotframework.org/robotframework/latest/libraries/Process.html#Result%20object
    [Arguments]    ${datastore}    ${candidate}
    ${result} =    Run Process
    ...    ${SDCTL}
    ...    -a    ${DATA-SERVER-IP}:${DATA-SERVER-PORT}
    ...    datastore    commit
    ...    --ds    ${datastore}
    ...    --candidate    ${candidate}

    Log    ${result.rc}
    Log    ${result.stdout}
    Log    ${result.stderr}
    RETURN    ${result}

Datastore Get Data
    [Documentation]    Get a path from the datastore
    [Arguments]    ${datastore}    @{ds-get-flags}
    ${result} =    Run Process
    ...    ${SDCTL}
    ...    -a    ${DATA-SERVER-IP}:${DATA-SERVER-PORT}
    ...    data    get
    ...    --ds    ${datastore}
    ...    @{ds-get-flags}

    Log    ${result.rc}
    Log    ${result.stdout}
    Log    ${result.stderr}
    RETURN    ${result}

GetFromCandidate
    [Documentation]    Get a path from the datastore candidate
    [Arguments]    ${datastore}    ${candidate}    ${path}
    ${result} =    Run Process
    ...    ${SDCTL}
    ...    -a    ${DATA-SERVER-IP}:${DATA-SERVER-PORT}
    ...    data    get
    ...    --ds    ${datastore}
    ...    --candidate    ${candidate}
    ...    --path    ${path}
    ...    --type    CONFIG
    ...    timeout=5sec

    Log    ${result.rc}
    Log    ${result.stdout}
    Log    ${result.stderr}
    RETURN    ${result}

Set
    [Documentation]    Applies to the candidate of the given datastore the provided update
    [Arguments]    ${datastore}    ${candidate}    @{set-flags}
    ${result} =    Run Process
    ...    ${SDCTL}
    ...    -a    ${DATA-SERVER-IP}:${DATA-SERVER-PORT}
    ...    data    set
    ...    --ds    ${datastore}
    ...    --candidate    ${candidate}
    ...    @{set-flags}

    Log    ${result.rc}
    Log    ${result.stdout}
    Log    ${result.stderr}
    RETURN    ${result}

SetIntent
    [Documentation]    Applies the given intent to the provided candidate datastore
    [Arguments]    ${datastore}    ${candidate}    ${intent}    ${priority}    ${file}    @{set-flags}
    ${result} =    Run Process
    ...    ${SDCTL}
    ...    -a    ${DATA-SERVER-IP}:${DATA-SERVER-PORT}
    ...    data    set-intent
    ...    --ds    ${datastore}
    ...    --candidate    ${candidate}
    ...    --priority    ${priority}
    ...    --intent    ${intent}
    ...    --file    ${file}
    ...    @{set-flags}

    Log    ${result.rc}
    Log    ${result.stdout}
    Log    ${result.stderr}
    RETURN    ${result}

DeleteIntent
    [Documentation]    Deletes the given intent from the datastore
    [Arguments]    ${datastore}    ${candidate}    ${intent}    ${priority}    @{set-flags}
    ${result} =    Run Process
    ...    ${SDCTL}
    ...    -a    ${DATA-SERVER-IP}:${DATA-SERVER-PORT}
    ...    data    set-intent
    ...    --ds    ${datastore}
    ...    --candidate    ${candidate}
    ...    --intent    ${intent}
    ...    --priority    ${priority}
    ...    --delete
    ...    @{set-flags}

    Log    ${result.rc}
    Log    ${result.stdout}
    Log    ${result.stderr}
    RETURN    ${result}

GetSchema
    [Documentation]    Retrieve the schema element described by name (plattform name), version and vendor under the given path.
    [Arguments]    ${name}    ${version}    ${vendor}    ${path}
    ${result} =    Run Process
    ...    ${SDCTL}
    ...    -a    ${SCHEMA-SERVER-IP}:${SCHEMA-SERVER-PORT}
    ...    schema    get
    ...    --name    ${name}
    ...    --version    ${version}
    ...    --vendor    ${vendor}
    ...    --path    ${path}

    Log    ${result.rc}
    Log    ${result.stdout}
    Log    ${result.stderr}
    RETURN    ${result}

ListSchemas
    ${result} =    Run Process
    ...    ${SDCTL}
    ...    -a
    ...    ${SCHEMA-SERVER-IP}:${SCHEMA-SERVER-PORT}
    ...    schema
    ...    list

    Log    ${result.rc}
    Log    ${result.stdout}
    Log    ${result.stderr}
    RETURN    ${result}

Check Datastore Connected
    [Documentation]    Checks weather the provided Datastore is in the connected state.
    [Arguments]    ${datastore}

    ${result} =    client.GetDataStore    ${datastore}
    Should Be Equal As Integers    ${result.rc}    0
    Should Contain    ${result.stdout}    name: "${datastore}"
    Should Contain    ${result.stdout}    status: CONNECTED
    RETURN    ${result}

Wait Until Datastore connected
    [Arguments]    ${datastore}    ${retry}    ${interval}
    Wait Until Keyword Succeeds    ${retry}    ${interval}    Check Datastore Connected    ${datastore}

# Helper

ExtractResponse
    [Documentation]    Takes the output of the client binary and returns just the response part, stripping the request
    [Arguments]    ${output}
    @{split} =    Split String    ${output}    response:
    RETURN    ${split}[1]

LogMustStatements
    [Documentation]    Takes vendor, name, version and a path, retrieves the schema for the given path, extracts the returned must_statements and logs them.
    [Arguments]    ${name}    ${version}    ${vendor}    ${path}
    ${schema} =    GetSchema    ${name}    ${version}    ${vendor}    ${path}
    ${msts} =    _ExtractMustStatements    ${schema.stdout}
    FOR    ${item}    IN    @{msts}
        Log    ${item}
    END

LogLeafRefStatements
    [Documentation]    Takes vendor, name, version and a path, retrieves the schema for the given path, extracts the returned leafref_statements and logs them.
    [Arguments]    ${name}    ${version}    ${vendor}    ${path}
    ${schema} =    GetSchema    ${name}    ${version}    ${vendor}    ${path}
    ${lref} =    _ExtractLeafRefStatements    ${schema.stdout}
    FOR    ${item}    IN    @{lref}
        Log    ${item}
    END

_ExtractMustStatements
    [Documentation]    Takes a GetSchema response and extracts the must_statements of the response. Returns an array with all the must_statements as a string array.
    [Arguments]    ${input}
    ${matches} =    Get Regexp Matches
    ...    ${input}
    ...    must_statements:\\s*\{[\\s\\S]*?\}
    ...    flags=MULTILINE | IGNORECASE
    RETURN    ${matches}

_ExtractLeafRefStatements
    [Documentation]    Takes a GetSchema response and extracts the leafref_statements of the response.
    [Arguments]    ${input}
    ${matches} =    Get Regexp Matches    ${input}    leafref:\\s*".*"    flags=IGNORECASE
    RETURN    ${matches}

Wait Until Datastore Data Contains
    [Documentation]    Issues sdctl data get for the given datastore, with the given path. Retries retry times for a maximum of interval time.
    ...    Thereby checking that all the provided containsStrings are contained in the output.
    [Arguments]    ${datastore}    ${path}    ${retry}    ${interval}    @{containStrings}
    Wait Until Keyword Succeeds
    ...    ${retry}
    ...    ${interval}
    ...    Check Datastore Data Get Contains
    ...    ${datastore}
    ...    ${path}
    ...    @{containStrings}

Check Datastore Data Get Contains
    [Documentation]    Issues sdctl data get for the given datastore, with the given path.
    ...    Thereby checking that all the provided containsStrings are contained in the output.
    [Arguments]    ${datastore}    ${path}    @{containStrings}
    ${result} =    Datastore Get Data    ${srlinux1-name}    --path    ${path}

    FOR    ${element}    IN    @{containStrings}
        Should Contain    ${result.stdout}    ${element}
    END
