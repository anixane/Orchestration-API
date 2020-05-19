"""
Module methods of ADF objects
"""

from flask import Flask, render_template, request, redirect, url_for
from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import *
from datetime import datetime, timedelta
import time


def print_item(group):
    """Print an Azure object instance."""
    print("\tName: {}".format(group.name))
    print("\tId: {}".format(group.id))
    if hasattr(group, 'location'):
        print("\tLocation: {}".format(group.location))
    if hasattr(group, 'tags'):
        print("\tTags: {}".format(group.tags))
    if hasattr(group, 'properties'):
        print_properties(group.properties)


def print_properties(props):
    """Print a ResourceGroup properties instance."""
    if props and hasattr(props, 'provisioning_state') and props.provisioning_state:
        print("\tProperties:")
        print("\t\tProvisioning State: {}".format(props.provisioning_state))
    print("\n\n")


def CreateADF(rg_name, df_name, df_params, adf_client):
    adf_identity = FactoryIdentity(
        type='SystemAssigned', principal_id=CLIENT_ID, tenant_id=TENANT_ID)
    df_resource = Factory(location='eastus', identity=adf_identity)
    df = adf_client.factories.create_or_update(rg_name, df_name, df_resource)
    while df.provisioning_state != 'Succeeded':
        df = adf_client.factories.get(rg_name, df_name)
        time.sleep(1)
    return df, adf_client


def CreateAzureKeyVaultLinkedService(rg_name, df_name, AKV_LS_NAME, keyvaultName, adf_client):
    ls_azure_storage = AzureKeyVaultLinkedService(
        type='str', base_url='https://' + keyvaultName + '.vault.azure.net/')
    ls = adf_client.linked_services.create_or_update(
        rg_name, df_name, AKV_LS_NAME, ls_azure_storage)
    print_item(ls)
    return ls, adf_client


def CreateAzureDataLakeStorageLinkedService(rg_name, df_name, ls_name, ADLSURI, adf_client, AKVsecret_name, AzureKVreference_name):
    lsr = LinkedServiceReference(
        type='LinkedServiceReference', reference_name=AzureKVreference_name)
    account_key = AzureKeyVaultSecretReference(
        type='str', store=lsr, secret_name=AKVsecret_name)
    ls_azure_storage = AzureDataLakeStoreLinkedService(
        data_lake_store_uri=ADLSURI, service_principal_id=CLIENT_ID, service_principal_key=account_key, tenant=TENANT_ID)
    ls = adf_client.linked_services.create_or_update(
        rg_name, df_name, ls_name, ls_azure_storage)
    print_item(ls)
    return ls, adf_client


def CreateParquetDataset(rg_name, df_name, ds_name, DATASET_FLODER_NAME, FOLDER_PATH, FILE_NAME, adf_client, ls_name, compression_TYPE='none'):
    ds_ls = LinkedServiceReference(reference_name=ls_name)
    DatasetLocation_adls = AzureDataLakeStoreLocation(
        folder_path=FOLDER_PATH, file_name=FILE_NAME)
    folder_dataset = DatasetFolder(name=DATASET_FLODER_NAME)
    ds_azure_adls = ParquetDataset(
        linked_service_name=ds_ls, folder=folder_dataset, type='str', location=DatasetLocation_adls, compression_codec=compression_TYPE)
    ds = adf_client.datasets.create_or_update(
        rg_name, df_name, ds_name, ds_azure_adls)
    print_item(ds)
    return ds, adf_client


def CreateCopyActivity(Activity_Name, Sourceadls, Sinkadls):
    ADLS_source = ParquetSource()
    ADLS_sink = ParquetSink()
    Sourcein_ref = DatasetReference(reference_name=Sourceadls)
    SinkOut_ref = DatasetReference(reference_name=Sinkadls)
    copy_activity = CopyActivity(name=Activity_Name, inputs=[Sourcein_ref], outputs=[
                                 SinkOut_ref], source=ADLS_source, sink=ADLS_sink)
    return copy_activity


def CreatePipeline(adf_client, rg_name, df_name, p_name, Activity=[]):
    params_for_pipeline = {}
    p_obj = PipelineResource(
        activities=Activity, parameters=params_for_pipeline)
    print(p_obj)
    p = adf_client.pipelines.create_or_update(rg_name, df_name, p_name, p_obj)
    return p, adf_client


def DeleteADF(adf_client, rg_name, df_name):
    adf_client.factories.delete(rg_name, df_name)


def DynamicADF(rg_name, df_name, subscription_id, TENANT_ID, CLIENT_ID, keyvaultName, AKVsecret_name, ADLSURI, SRC_FOLDER_PATH, SINK_FOLDER_PATH, FILE_NAME):
    credentials = ServicePrincipalCredentials(client_id=CLIENT_ID,
                                              secret='..6AmP?Osx2M4/UU40Hd[0LE]Y@BxqdS', tenant=TENANT_ID)
    adf_client = DataFactoryManagementClient(credentials, subscription_id)

    rg_params = {'location': 'eastus'}
    df_params = {'location': 'eastus'}

    adf, adf_client = CreateADF(rg_name, df_name, df_params, adf_client)

    AzureKeyVaultLinkedService, adf_client = CreateAzureKeyVaultLinkedService(
        rg_name, df_name, 'AzureKeyVaultLinkedService', keyvaultName, adf_client)

    AzureDataLakeStorageLinkedService, adf_client = CreateAzureDataLakeStorageLinkedService(
        rg_name, df_name, 'AzureDataLakeStorageLinkedService', ADLSURI, adf_client, AKVsecret_name, 'AzureKeyVaultLinkedService')

    SourceprqDataset, adf_client = CreateParquetDataset(rg_name, df_name, 'SourceprqDataset', 'staging',
                                                        SRC_FOLDER_PATH, FILE_NAME, adf_client, 'AzureDataLakeStorageLinkedService', compression_TYPE='none')

    SinkprqDataset, adf_client = CreateParquetDataset(
        rg_name, df_name, 'SinkprqDataset', 'staging', SINK_FOLDER_PATH, FILE_NAME, adf_client, 'AzureDataLakeStorageLinkedService', compression_TYPE='none')
    copy_activity = CreateCopyActivity(
        'COPYparquetfile', 'SourceprqDataset', 'SinkprqDataset')

    pipeline, adf_client = CreatePipeline(
        adf_client, rg_name, df_name, 'CopyAdlsToAdls', [copy_activity])
    return adf_client


app = Flask(__name__)
# defining global variables
rg_name = 'OrchestrationTestResourceGroup'
df_name = 'testadfnew08'
subscription_id = '5fd4b8a3-3bd7-4592-89ba-6a56ace0a00b'
TENANT_ID = '7d46cab4-943a-4332-8961-e7fbce0633cf'
CLIENT_ID = '88ad9879-d7d1-4ce0-b3b9-e1248ed6ebc9'
keyvaultName = 'orchestrationkeyvault'
AKVsecret_name = 'client-secret'
ADLSURI = 'adl://stagingadls.azuredatalakestore.net'
SRC_FOLDER_PATH = ''
SINK_FOLDER_PATH = 'Output'
FILE_NAME = 'DimAccount'
SourceSTORAGE_TYPE = 'ADLS'


@app.route('/', methods=["GET", "POST"])
def landingPage():
    return render_template('index.html')


@app.route('/credentialsForm', methods=["GET", "POST"])
def credentialsForm():
    global TENANT_ID, subscription_id, CLIENT_ID, keyvaultName, rg_name, df_name
    if request.method == 'POST':
        response = request.form
        TENANT_ID = response['TenantID']
        subscription_id = response['SubscriptionID']
        CLIENT_ID = response['AADAppClientID']
        keyvaultName = response['AzureKeyVaultName']
        rg_name = response['ResourceGroupName']
        df_name = response['AzureDataFactoryName']
        return redirect(url_for('stagingDestinationForm'))
    return render_template('credentialsForm.html')


@app.route('/stagingDestinationForm', methods=["GET", "POST"])
def stagingDestinationForm():
    global ADLSURI, SINK_FOLDER_PATH, AKVsecret_name, SRC_FOLDER_PATH
    if request.method == 'POST':
        response = request.form
        ADLSURI = response['stagingDestinationADLSURL']
        SINK_FOLDER_PATH = response['stagingDestADLSFolderPath']
        AKVsecret_name = response['stagingDestAzureKeyVaultSecretName']
        return redirect(url_for('stagingUpstreamForm'))
    return render_template('stagingDestinationForm.html')


@app.route('/stagingUpstreamForm', methods=["GET", "POST"])
def stagingUpstreamForm():
    global SourceSTORAGE_TYPE, AKVsecret_name, ADLSURI, SRC_FOLDER_PATH, FILE_NAME, rg_name, df_name, subscription_id, TENANT_ID, CLIENT_ID, keyvaultName, SINK_FOLDER_PATH, FILE_NAME
    if request.method == 'POST':
        response = request.form
        SourceSTORAGE_TYPE = response['UpstreamStorageType']
        AKVsecret_name = response['stagingUpstreamAzureKeyVaultSecretName']
        ADLSURI = response['stagingUpstreamADLSURL']
        SRC_FOLDER_PATH = response['stagingUpstreamADLSFolderPath']
        FILE_NAME = response['stagingUpstreamADLSFileName']

        DynamicADF(rg_name, df_name, subscription_id, TENANT_ID, CLIENT_ID, keyvaultName,
                   AKVsecret_name, ADLSURI, SRC_FOLDER_PATH, SINK_FOLDER_PATH, FILE_NAME)

        return render_template('successfulDeploy.html', DataFactory_Name=str(df_name))
    return render_template('stagingUpstreamForm.html')


if __name__ == '__main__':
    app.run(debug=True)
