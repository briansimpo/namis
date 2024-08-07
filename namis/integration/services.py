
import json
import csv
import logging
import os
import requests
from django.conf import settings
from django.core.files.storage import default_storage
from requests.auth import HTTPBasicAuth
from datetime import datetime
from types import SimpleNamespace
from .util import JsonObject, to_bool

logger = logging.getLogger(__name__)

class API:

    api_url = settings.NAMIS_API
    api_username = settings.NAMIS_USERNAME
    api_password = settings.NAMIS_PASSWORD

    headers = {'Content-Type': 'application/json'}

    def __init__(self):
        self.auth = HTTPBasicAuth(self.api_username, self.api_password)

    def _build(self, endpoint):
        return f"{self.api_url}/{endpoint}"
    
    def post(self, endpoint, payload):
        endpoint = self._build(endpoint)
        response = requests.post(url=endpoint, json=payload, auth=self.auth, headers=self.headers)
        return response.json()

class Namis:

    entity_type = "JXqDBe1cNcL" # Farmer
    program = "Y4g6aGReECE" # Farm Household Register
    household_stage= "AS1r4HWv36F" # Program Stage ID
    farming_overview_stage = "bKtLxsPhyQF" # Program Stage ID
    farming_method_stage = "GDCkEHh2rrP" # Program Stage ID
    support_stage = "iAE7BP6fBXf" # Program Stage ID

    profile_endpoint = "trackedEntityInstances"
    enrollment_endpoint = "enrollments"
    events_endpoint = "events"

    error = None

    def __init__(self, record) -> None:
        self.record = record
        self.api = API()
    
    def _dict_to_object(self, data):
        return json.loads(json.dumps(data), object_hook=lambda d: SimpleNamespace(**d))

    def _get_current_date(self):
        current_datetime = datetime.now()
        return current_datetime.strftime("%Y-%m-%d")

    def _post_profile(self, entity_type, org_unit, data):
        payload = {
            "trackedEntityType": entity_type, 
            "orgUnit": org_unit,
            "attributes": [
                {
                    "displayName": "SerialNumber",
                    "attribute": "c3aKgEhckfB",
                    "value": None
                },
                {
                    "displayName": "NationalIDQRCode",
                    "attribute": "bGCIwhx0QWQ",
                    "value": str(data["NationalIDQRCode"])
                },
                {
                    "displayName": "NationalID",
                    "attribute": "W6kJs5es1rY",
                    "value": str(data["NationalID"])
                },
                {
                    "displayName": "HouseholdHead",
                    "attribute": "znJuOdk5Cdl",
                    "value": str(data["HouseholdHead"])
                },
                {
                    "displayName": "Birthday",
                    "attribute": "gEjOqoZAviX",
                    "value": data["Birthday"]
                },
                {
                    "displayName": "Sex",
                    "attribute": "ij3vRdw9lju",
                    "value": str(data["Sex"])
                },
                {
                    "displayName": "PhoneNumber",
                    "attribute": "fdrAPSn6xfz",
                    "value": str(data["PhoneNumber"])
                },
                {
                    "displayName": "PhoneType",
                    "attribute": "tL4Iss9KsVC",
                    "value": str(data["PhoneType"])
                },
                {
                    "displayName": "Education",
                    "attribute": "Qi0SsrtddJp",
                    "value": str(data["Education"])
                },
                {
                    "displayName": "Occupation",
                    "attribute": "vjeRjLGKjHV",
                    "value": str(data["Occupation"])
                },
                
                {
                    "displayName": "HeadCondition",
                    "attribute": "aJEyk9gIxcr",
                    "value": str(data["HeadCondition"])
                },
                {
                    "displayName": "HouseholdCoordinates",
                    "attribute": "EHBa25SkWJg",
                    "value": None
                },
                {
                    "displayName": "ADD",
                    "attribute": "EW2GONAplcf",
                    "value": str(data["ADD"])
                },
                {
                    "displayName": "District",
                    "attribute": "SuWgGzt14Je",
                    "value": str(data["District"])
                },
                {
                    "displayName": "Constituency",
                    "attribute": "GcB2oLrzWqb",
                    "value": str(data["Constituency"])
                },
                {
                    "displayName": "TA",
                    "attribute": "xRmOhRvfniD",
                    "value": str(data["TA"])
                },
                {
                    "displayName": "GVH",
                    "attribute": "HmENbD9wbaf",
                    "value": str(data["GVH"])
                },
                {
                    "displayName": "NearestAdmarc",
                    "attribute": "ffoDQT1tJfz",
                    "value": str(data["NearestAdmarc"])
                },
                {
                    "displayName": "NearestMarket",
                    "attribute": "QEzPjaQKA54",
                    "value": str(data["NearestMarket"])
                }
            ]
        }
        response = self.api.post(self.profile_endpoint, payload)
        result  = JsonObject(response)
        reference = result.response.importSummaries[0].reference
        if reference:
            return reference
        else:
            self.error = response['response']['importSummaries'][0]['conflicts'][0]['value']
                    
    def _post_enrollment(self, entity_instance, org_unit):
        current_date = self._get_current_date()

        payload = {
            "trackedEntityInstance": entity_instance,
            "program": self.program,
            "orgUnit": org_unit,
            "enrollmentDate": current_date,
            "incidentDate": current_date
         }
        return self.api.post(self.enrollment_endpoint, payload)

    def _post_household_demographics(self, entity_instance, org_unit, data):
        current_date = self._get_current_date()

        payload = {
            "program": self.program,
            "orgUnit": org_unit,
            "eventDate": current_date,
            "status": "COMPLETED",
            "trackedEntityInstance": entity_instance,
            "programStage": self.household_stage,
            "dataValues": [
                { 
                    "displayName": "HouseholdSize",
                    "dataElement": "V9ohTgE9gt6", 
                    "value": str(data["HouseholdSize"]) 
                },
                
                { 
                    "displayName": "UnderFiveChildren",
                    "dataElement": "LODyndrVqqy", 
                    "value": str(data["UnderFiveChildren"]) 
                },
                
                { 
                    "displayName": "FarmingParticipants",
                    "dataElement": "sdhYSHX6ADi", 
                    "value": str(data["FarmingParticipants"]) 
                },
                
                { 
                    "displayName": "Disabilities",
                    "dataElement": "lUj8KABoecG", 
                    "value": str(data["Disabilities"]) 
                },
                
                { 
                    "displayName": "FarmingIncome",
                    "dataElement": "jhsAI5u7Qtz", 
                    "value": str(data["FarmingIncome"]) 
                },
                
                { 
                    "displayName": "OverallIncome",
                    "dataElement": "wFBmGcv8oD5", 
                    "value": str(data["OverallIncome"]) 
                },
                
                { 
                    "displayName": "MaritalStatus",
                    "dataElement": "cxGvtTqFZRg", 
                    "value": str(data["MaritalStatus"])
                },
                
                { 
                    "displayName": "PrimaryIncomeSource",
                    "dataElement": "RY1Sjzog4uV", 
                    "value": str(data["PrimaryIncomeSource"])
                },
                
                { 
                    "displayName": "SpouseName",
                    "dataElement": "gESUD9F82QZ", 
                    "value": str(data["SpouseName"])
                },
                
                {
                    "displayName": "SpouseNationalIDQRCode", 
                    "dataElement": "f69kxpGfwar", 
                    "value": None
                },
                
                { 
                    "displayName": "SpouseNationalID",
                    "dataElement": "Qh0LStKoviT", 
                    "value": None
                },
                
                {
                    "displayName": "SpouseBirthday", 
                    "dataElement": "uE408f0Mjlv", 
                    "value": None
                }
            
            ]
        }

        return self.api.post(self.events_endpoint, payload)

    def _post_farming_overview(self, entity_instance, org_unit, data):
        current_date = self._get_current_date()

        payload = {
            "program": self.program,
            "orgUnit": org_unit,
            "eventDate": current_date,
            "status": "COMPLETED",
            "trackedEntityInstance": entity_instance,
            "programStage": self.farming_overview_stage,
            "dataValues": [
                { 
                    "displayName": "FarmerGroupMember",
                    "dataElement": "QqwVUXU3hGd", 
                    "value": to_bool(data["FarmerGroup"]) 
                },

                { 
                    "displayName": "FarmerGroupName",
                    "dataElement": "YhGb4g7jTSA", 
                    "value": str(data["FarmerGroupName"])
                },
                
                {
                    "displayName": "RentedLandSize", 
                    "dataElement": "cpEikdJudnS", 
                    "value": None 
                },
                
                { 
                    "displayName": "PermanentLandSize",
                    "dataElement": "aVI6OByBS4R", 
                    "value": None
                },
                
                { 
                    "displayName": "MaiFieldGPS",
                    "dataElement": "v3Yo1rNuGwq", 
                    "value": None
                },
                
                { 
                    "displayName": "LandLegalStatus",
                    "dataElement": "fmUHqcHA8T7", 
                    "value": None
                },
                
                { 
                    "displayName": "TotalLandSize",
                    "dataElement": "aNCtzVQCjK4", 
                    "value": str(data["TotalLandSize"])
                }
            ]
        }
        return self.api.post(self.events_endpoint, payload)

    def _post_support(self, entity_instance, org_unit, data):
        current_date = self._get_current_date()

        payload = {
            "program": self.program,
            "orgUnit": org_unit,
            "eventDate": current_date,
            "status": "COMPLETED",
            "trackedEntityInstance":entity_instance,
            "programStage": self.support_stage,
            "dataValues": [
                {
                    "displayName": "CreditService",
                    "dataElement": "wJlsR1WLgbO",
                    "value": to_bool(data["CreditService"])
                },
                
                {
                    "displayName": "CreditServiceProvider",
                    "dataElement": "xy26yMuqVC8",
                    "value": str(data["CreditServiceProvider"])
                },
                
                
                {
                    "displayName": "Extension_FaceToFace",
                    "dataElement": "QXL27V3TVYU",
                    "value": to_bool(data["Extension_FaceToFace"])
                },
                {
                    "displayName": "Extension_SocialMedia",
                    "dataElement": "OK7D6F0QUUS",
                    "value": to_bool(data["Extension_SocialMedia"])
                },
                {
                    "displayName": "Extension_Radios",
                    "dataElement": "LS007281CI9",
                    "value": to_bool(data["Extension_Radios"])
                },
                {
                    "displayName": "Extension_Posters",
                    "dataElement": "WER8M7KU1B7",
                    "value": to_bool(data["Extension_Posters"])
                },
                {
                    "displayName": "Extension_FellowFarmers",
                    "dataElement": "DNW7A591194",
                    "value": to_bool(data["Extension_FellowFarmers"])
                },
                {
                    "displayName": "Extension_AgroSuppliers",
                    "dataElement": "MFV29UFT85W",
                    "value": to_bool(data["Extension_AgroSuppliers"])
                },

                {
                    "displayName": "PreferredMode_FaceToFace",
                    "dataElement": "KYIG1490L46",
                    "value": to_bool(data["PreferredMode_FaceToFace"])
                },
                {
                    "displayName": "PreferredMode_SocialMedia",
                    "dataElement": "YHHTQ18U4XO",
                    "value": to_bool(data["PreferredMode_SocialMedia"])
                },
                {
                    "displayName": "PreferredMode_Radios",
                    "dataElement": "VATJC3AVWA0",
                    "value": to_bool(data["PreferredMode_Radios"])
                },
                {
                    "displayName": "PreferredMode_Posters",
                    "dataElement": "EYH70MDZT88",
                    "value": to_bool(data["PreferredMode_Posters"])
                },
                {
                    "displayName": "PreferredMode_FellowFarmers",
                    "dataElement": "HM27I955MNO",
                    "value": to_bool(data["PreferredMode_FellowFarmers"])
                },
                {
                    "displayName": "PreferredMode_AgroSuppliers",
                    "dataElement": "RO9J6CV17MU",
                    "value": to_bool(data["PreferredMode_AgroSuppliers"])
                },

                {
                    "displayName": "ReceiptOfSupport",
                    "dataElement": "jVcTS9kdNwU",
                    "value": to_bool(data["ReceiptOfSupport"])
                },
                {
                    "displayName": "SourceOfSupport",
                    "dataElement": "OyIgAUaoduw",
                    "value": str(data["SourceOfSupport"])
                },
                {
                    "displayName": "SupportOrganization",
                    "dataElement": "WLn93SWcfRV",
                    "value": str(data["SupportOrganization"])
                },
                {
                    "displayName": "SupportDuration",
                    "dataElement": "QYlx3R6TQ2q",
                    "value": str(data["SupportDuration"])
                },
                {
                    "displayName": "Support_Cash",
                    "dataElement": "UWH170UE2H6",
                    "value": to_bool(data["Support_Cash"])
                },
                {
                    "displayName": "Support_Seeds",
                    "dataElement": "KZ9499349G4",
                    "value": to_bool(data["Support_Seeds"])
                },
                {
                    "displayName": "Support_Fertilizer",
                    "dataElement": "AP0P42X1X24",
                    "value":  to_bool(data["Support_Fertilizer"])
                },
                {
                    "displayName": "Support_ExtensionService",
                    "dataElement": "NCIZGNQ69IX",
                    "value":  to_bool(data["Support_ExtensionService"])
                },
                {
                    "displayName": "Support_Livestock",
                    "dataElement": "SI34R4K3YLL",
                    "value":  to_bool(data["Support_Livestock"])
                },
                {
                    "displayName": "Support_LandManagement",
                    "dataElement": "VN40U24X7TM",
                    "value":  to_bool(data["Support_LandManagement"])
                },
                {
                    "displayName": "Support_Nutrition",
                    "dataElement": "FM7H5Z11ZRH",
                    "value":  to_bool(data["Support_Nutrition"])
                },
                {
                    "displayName": "ReceiptOfExtraSupport",
                    "dataElement": "YsUioHiJjfg",
                    "value":  to_bool(data["ReceiptOfExtraSupport"])
                },
                {
                    "displayName": "SourceOfExtraSupport",
                    "dataElement": "myv9cIOKZKe",
                    "value":  None
                },
                {
                    "displayName": "ExtraSupportOrganization",
                    "dataElement": "FCp7LyZF4yN",
                    "value":  str(data["ExtraSupportOrganization"])
                },
                {
                    "displayName": "ExtraSupportDuration",
                    "dataElement": "IuOQR1VnCYw",
                    "value":  str(data["ExtraSupportDuration"])
                },
                {
                    "displayName": "ExtraSupport_Cash",
                    "dataElement": "YMV5FN1JW6H",
                    "value":  to_bool(data["ExtraSupport_Cash"])
                },
                {
                    "displayName": "ExtraSupport_Seeds",
                    "dataElement": "HN42ASU8USI",
                    "value":  to_bool(data["ExtraSupport_Seeds"])
                },
                {
                    "displayName": "ExtraSupport_Fertilizer",
                    "dataElement": "ZRQP7E5U1WG",
                    "value":  to_bool(data["ExtraSupport_Fertilizer"])
                },
                {
                    "displayName": "ExtraSupport_ExtensionService",
                    "dataElement": "SJPSQ28ET7U",
                    "value":  to_bool(data["ExtraSupport_ExtensionService"])
                },
                {
                    "displayName": "ExtraSupport_Livestock",
                    "dataElement": "LD1KT343F5T",
                    "value":  to_bool(data["ExtraSupport_Livestock"])
                },
                {
                    "displayName": "ExtraSupport_LandManagement",
                    "dataElement": "SO3326C6N4G",
                    "value":  to_bool(data["ExtraSupport_LandManagement"])
                },
                {
                    "displayName": "ExtraSupport_Nutrition",
                    "dataElement": "CHSTCBKLX69",
                    "value":  to_bool(data["ExtraSupport_Nutrition"])
                }

                
            ]
        }
        return self.api.post(self.events_endpoint, payload)

    def _post_farming_method(self, entity_instance, org_unit, data):
        current_date = self._get_current_date()
        payload = {
            "program": self.program,
            "orgUnit": org_unit,
            "eventDate": current_date,
            "status": "COMPLETED",
            "trackedEntityInstance": entity_instance,
            "programStage": self.farming_method_stage,
            "dataValues": [
                {
                    "displayName": "UseIrrigation",
                    "dataElement": "r0IkPiagdvQ",
                    "value": to_bool(data["UseIrrigation"])
                },
                
                {
                    "displayName": "IrrigationType",
                    "dataElement": "CXYfteuVgcQ",
                    "value": str(data["IrrigationType"])
                },
                {
                    "displayName": "IrrigationMethod",
                    "dataElement": "EK8O4uHpm28",
                    "value": str(data["IrrigationMethod"])
                },
                {
                    "displayName": "EnergySource",
                    "dataElement": "tB4VCxeipsv",
                    "value": str(data["EnergySource"])
                },
                {
                    "displayName": "WaterSource",
                    "dataElement": "GhFwhm9aVTG",
                    "value": str(data["WaterSource"])
                },
                {
                    "displayName": "SurfaceWaterSource",
                    "dataElement": "DDPpFvAgQ06",
                    "value": str(data["SurfaceWaterSource"])
                },
                {
                    "displayName": "SubsurfaceWaterSource",
                    "dataElement": "ejvXcsevL6X",
                    "value": str(data["SubsurfaceWaterSource"])
                },
                {
                    "displayName": "EnterpriseType",
                    "dataElement": "VlXtRS3X8vg",
                    "value": str(data["EnterpriseType"])
                },
                {
                    "displayName": "Maize",
                    "dataElement": "GUvE51AL6sI",
                    "value": to_bool(data["Maize"])
                },
                {
                    "displayName": "Millet",
                    "dataElement": "m2k52GtcsEv",
                    "value": to_bool(data["Millet"])
                },
                {
                    "displayName": "Okra",
                    "dataElement": "BwCQIRjaNKt",
                    "value": to_bool(data["Okra"])
                },
                {
                    "displayName": "Onions",
                    "dataElement": "jqAOwrpHG6J",
                    "value": to_bool(data["Onions"])
                },
                {
                    "displayName": "Papaya",
                    "dataElement": "zoskEfLmDA4",
                    "value": to_bool(data["Papaya"])
                },
                {
                    "displayName": "PigeonPeas",
                    "dataElement": "Lb70D9e8Cvz",
                    "value": to_bool(data["PigeonPeas"])
                },
                {
                    "displayName": "Pineapple",
                    "dataElement": "f6Q8V7FOFHK",
                    "value": to_bool(data["Pineapple"])
                },
                {
                    "displayName": "Pumpkin",
                    "dataElement": "oGlMqxJZTCU",
                    "value": to_bool(data["Pumpkin"])
                },
                {
                    "displayName": "Rice",
                    "dataElement": "QyMKnDBNcbu",
                    "value": to_bool(data["Rice"])
                },
                {
                    "displayName": "Sesame",
                    "dataElement": "VrvMIi1hUpo",
                    "value": to_bool(data["Sesame"])
                },
                {
                    "displayName": "Sorghum",
                    "dataElement": "CZFVe8qyIsM",
                    "value": to_bool(data["Sorghum"])
                },
                {
                    "displayName": "Soyabean",
                    "dataElement": "dGrPZZUxWCo",
                    "value": to_bool(data["Soyabean"])
                },
                {
                    "displayName": "Sugarcane",
                    "dataElement": "UnBHTZiHPWH",
                    "value": to_bool(data["Sugarcane"])
                },
                {
                    "displayName": "Sunflower",
                    "dataElement": "ePrhycaXAKN",
                    "value": to_bool(data["Sunflower"])
                },
                {
                    "displayName": "SweetPotato",
                    "dataElement": "HK4bZn7FpQL",
                    "value": to_bool(data["SweetPotato"])
                },
                {
                    "displayName": "Tomatoes",
                    "dataElement": "Ks0MuqEbwa3",
                    "value": to_bool(data["Tomatoes"])
                },
                {
                    "displayName": "Wheat",
                    "dataElement": "RXIclGKl3uq",
                    "value": to_bool(data["Wheat"])
                },
                {
                    "displayName": "MainFoodCropOutput",
                    "dataElement": "pg4jYMQwUPA",
                    "value": str(data["MainFoodCropOutput"])
                },
                {
                    "displayName": "Pestcontrol_Fungicides",
                    "dataElement": "LQ72O792KNB",
                    "value": to_bool(data["Pestcontrol_Fungicides"])
                },
                {
                    "displayName": "Pestcontrol_Rodenticides",
                    "dataElement": "VL74L2S4K7I",
                    "value": to_bool(data["Pestcontrol_Rodenticides"])
                },
                {
                    "displayName": "Pestcontrol_Insecticides",
                    "dataElement": "TR34G1G970S",
                    "value": to_bool(data["Pestcontrol_Insecticides"])
                },
                {
                    "displayName": "Pestcontrol_Molluscicides",
                    "dataElement": "MM9529F1695",
                    "value": to_bool(data["Pestcontrol_Molluscicides"])
                },
                {
                    "displayName": "Pestcontrol_Herbicides",
                    "dataElement": "MAJ86DJ210P",
                    "value": to_bool(data["Pestcontrol_Herbicides"])
                },
                {
                    "displayName": "Pestcontrol_BiologicalMethods",
                    "dataElement": "DK7G9XYY3O1",
                    "value": to_bool(data["Pestcontrol_BiologicalMethods"])
                },
                {
                    "displayName": "Pestcontrol_TraditionalMethods",
                    "dataElement": "GIC4WU397A9",
                    "value": to_bool(data["Pestcontrol_TraditionalMethods"])
                },
               
                {
                    "displayName": "Fertilizer_NPK",
                    "dataElement": "VF57Y1H90RR",
                    "value": to_bool(data["Fertilizer_NPK"])
                },
                {
                    "displayName": "Fertilizer_Urea",
                    "dataElement": "ONO7YG2DE45",
                    "value": to_bool(data["Fertilizer_Urea"])
                },
                {
                    "displayName": "Fertilizer_DAP",
                    "dataElement": "YZ94AP6367R",
                    "value": to_bool(data["Fertilizer_DAP"])
                },
                {
                    "displayName": "Fertilizer_OrganicManure",
                    "dataElement": "PD7M4XH7V1B",
                    "value": to_bool(data["Fertilizer_OrganicManure"])
                },
                {
                    "displayName": "Fertilizer_Hybrid",
                    "dataElement": "XUR592HS474",
                    "value": to_bool(data["Fertilizer_Hybrid"])
                },
                {
                    "displayName": "Fertilizer_SulphateOfAmmonium",
                    "dataElement": "XPT97TXC41K",
                    "value": to_bool(data["Fertilizer_SulphateOfAmmonium"])
                },
                {
                    "displayName": "Fertilizer_CAN",
                    "dataElement": "NNNIR789V4A",
                    "value": to_bool(data["Fertilizer_CAN"])
                },
                {
                    "displayName": "Fertilizer_SuperD",
                    "dataElement": "AD3W70P32Y1",
                    "value": to_bool(data["Fertilizer_SuperD"])
                },
                {
                    "displayName": "Fertilizer_DCompound",
                    "dataElement": "UR897V0HG36",
                    "value": to_bool(data["Fertilizer_DCompound"])
                },
                
                {
                    "displayName": "SeedMultiplication",
                    "dataElement": "VG27WiA9zd1",
                    "value": to_bool(data["SeedMultiplication"])
                },
                {
                    "displayName": "KeepLivestock",
                    "dataElement": "fn0M0La8dyl",
                    "value": to_bool(data["KeepLivestock"])
                },
                {
                    "displayName": "MainPastureLand",
                    "dataElement": "ejvXcsevL6X",
                    "value": str(data["MainPastureLand"])
                },
                {
                    "displayName": "Cattle",
                    "dataElement": "u43vGnAw8Wa",
                    "value": to_bool(data["Cattle"])
                },
                {
                    "displayName": "Goat",
                    "dataElement": "V4afoGT13Ko",
                    "value": to_bool(data["Goat"])
                },
                {
                    "displayName": "Bees",
                    "dataElement": "wkrIjG5Wlzq",
                    "value": to_bool(data["Bees"])
                },
                {
                    "displayName": "Chicken",
                    "dataElement": "gseaJTN0Ez1",
                    "value": to_bool(data["Chicken"])
                },
                {
                    "displayName": "Ducks",
                    "dataElement": "Ew9OkncLxzt",
                    "value": to_bool(data["Ducks"])
                },
                {
                    "displayName": "GuineaFowls",
                    "dataElement": "OoUdxiz4jgP",
                    "value": to_bool(data["GuineaFowls"])
                },
                {
                    "displayName": "GuineaPigs",
                    "dataElement": "QpoHNfefGs9",
                    "value": to_bool(data["GuineaPigs"])
                },
                {
                    "displayName": "Pigeon",
                    "dataElement": "ZZfUJKi4pDQ",
                    "value": to_bool(data["Pigeon"])
                },
                {
                    "displayName": "Pigs",
                    "dataElement": "pbJraIqWG7a",
                    "value": to_bool(data["Pigs"])
                },
                {
                    "displayName": "Quills",
                    "dataElement": "b067T9mhtsF",
                    "value": str(data["Quills"])
                },
                {
                    "displayName": "Rabbits",
                    "dataElement": "KeGhTSbpM2P",
                    "value": to_bool(data["Rabbits"])
                },
                {
                    "displayName": "Sheep",
                    "dataElement": "IIHE7akabKi",
                    "value": to_bool(data["Sheep"])
                },
                {
                    "displayName": "Turkey",
                    "dataElement": "Vi1V5duySTC",
                    "value": to_bool(data["Turkey"])
                },
                {
                    "displayName": "FishFarmingPractice",
                    "dataElement": "NlDVmTM0Xne",
                    "value": to_bool(data["FishFarmingPractice"])
                },
                {
                    "displayName": "FishFarmingPurpose",
                    "dataElement": "UvwWIPlR73A",
                    "value": str(data["FishFarmingPurpose"])
                },
                {
                    "displayName": "ProductionPurpose",
                    "dataElement": "uzEJJETmBoM",
                    "value": None
                },
                {
                    "displayName": "LabourSource",
                    "dataElement": "s6uNwAugtU5",
                    "value": str(data["LabourSource"])
                }
            ]
        }

        return self.api.post(self.events_endpoint, payload)

    def post(self):
        org_unit = self.record["Blocks"]
        
        entity_instance = self._post_profile(entity_type=self.entity_type, org_unit=org_unit, data=self.record)

        self._post_enrollment(entity_instance=entity_instance, org_unit=org_unit)
        self._post_household_demographics(entity_instance=entity_instance, org_unit=org_unit, data=self.record)
        self._post_farming_overview(entity_instance=entity_instance, org_unit=org_unit, data=self.record)
        self._post_support(entity_instance=entity_instance, org_unit=org_unit, data=self.record)
        self._post_farming_method(entity_instance=entity_instance, org_unit=org_unit, data=self.record)

        return entity_instance, self.error

class Processor:
    posted_file = "logs/posted.csv"
    failed_file = "logs/failed.csv"
    log_file = "logs/errors.log"

    def upload(self, filepath):
        logger.info("Process Initiated")
        with default_storage.open(filepath, mode='r') as file:
            self._process(file)         
        logger.info("Process Completed")

    def read(self, filepath):
        logger.info("Process Initiated")
        with open(filepath, mode='r', newline='') as file:
            self._process(file)  
        logger.info("Process Completed")

    def _process(self, file):
        reader = csv.DictReader(file)
        counter = 0
        for row in reader:
            namis = Namis(row)
            result, error = namis.post()
            counter += 1
            if result:
                self._write(data=row,filepath=self.posted_file)
                logger.info(f"Row: {counter}, Reference: {result}, Status: Success")
            else:
                message = f"Row: {counter}, Error: {error}"
                self._write(data=row, filepath=self.failed_file)
                self._log(message)
                logger.error(message)    

    def _log(self, message):
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        formatted_message = f"{current_time} - ERROR - {message}\n"
        with open(self.log_file, 'a') as file:
            file.write(formatted_message)

    def _write(self, data, filepath, mode='a'):
        file_exists = os.path.isfile(filepath)
        with open(filepath, mode=mode, newline='') as file:
            writer = csv.DictWriter(file, fieldnames=data.keys())
            if not file_exists:
                writer.writeheader()
            writer.writerow(data)
