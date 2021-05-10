import arcpy, os, zipfile, traceback, shutil, ConfigParser, sys, xml.etree.ElementTree as ET, time
from collections import OrderedDict
from datetime import datetime

try:

    StartTime = datetime.now().replace(microsecond=0)

    # Assign config path
    root_path = os.path.dirname(os.path.dirname(__file__))
    ini_path = os.path.join(root_path, 'ini', 'dcm_config.ini')

    # Set configuration file path
    config = ConfigParser.ConfigParser()
    config.read(ini_path)

    # Set log file path
    log_path = config.get('PATHS', 'log_path')
    log = open(os.path.join(log_path, 'DCM_SDE_distribution_log.txt'), "a")

    # Set necessary connection and directory paths and date variable
    print("Setting output directory paths")

    disconnect_sde = config.get("VARS", "sde_user_disconnect")
    publication_date = config.get("VARS", "publication_date")
    pubdate = datetime.strptime(publication_date, '%Y%m%d')
    pub_yr = str(pubdate.year)
    pub_longform = pubdate.strftime("%B %d, %Y")
    translator = config.get('PATHS', 'translator')
    xslt_html = config.get('PATHS', 'xslt_html')
    xslt_lcl_strg = config.get('PATHS', 'xslt_lcl_strg')
    xslt_geoproc_hist = config.get('PATHS', 'xslt_geoproc_hist')
    sde_trd_path = config.get('PATHS', 'sde_trd_path')
    sde_prod_path = config.get('PATHS', 'sde_prod_path')
    sde_dzm_path = os.path.join(sde_trd_path, 'GISTRD.TRD.Digital_Zoning_Map')
    lyr_dcm_path = config.get('PATHS', 'lyr_dcm_path')
    localtemp_path = config.get("PATHS", "export_path")
    current_export_path = os.path.join(localtemp_path, pub_yr, publication_date)
    template_path = config.get('PATHS', 'template_path')

    meta_path = os.path.join(current_export_path, "meta")

    # Check temp dir existence
    if os.path.exists(os.path.join(localtemp_path, pub_yr)):
        print("{} path exists. Skipping".format(pub_yr))
    else:
        os.mkdir(os.path.join(localtemp_path, pub_yr))

    dcm_export_list = os.listdir(os.path.join(localtemp_path, pub_yr))
    print(dcm_export_list)

    if publication_date in dcm_export_list:
        print("{} path exists. Skipping".format(current_export_path))
    else:
        os.mkdir(current_export_path)

    if os.path.exists(meta_path):
        print("Meta path exists. Skipping")
    else:
        os.mkdir(meta_path)

    # Define TRD SDE feature class paths
    print("Setting input data paths")
    DCM_path = os.path.join(sde_dzm_path, 'GISTRD.TRD.DCM')
    DCM_SCL_path = os.path.join(sde_dzm_path, 'GISTRD.TRD.DCM_SCL')
    DCM_NYMI_path = os.path.join(sde_dzm_path, 'GISTRD.TRD.DCM_nymi')
    DCM_ArterialMajorSts_path = os.path.join(sde_dzm_path, 'GISTRD.TRD.DCM_Arterials_Mjr_Sts')
    DCM_Area_StreetNameChg_path = os.path.join(sde_dzm_path, 'GISTRD.TRD.DCP_TRD_StreetNameChanges_Areas')
    DCM_Points_StreetNameChg_path = os.path.join(sde_dzm_path, 'GISTRD.TRD.DCP_TRD_StreetNameChanges_Points')
    DCM_Lines_StreetNameChg_path = os.path.join(sde_dzm_path, 'GISTRD.TRD.DCM_NmChng_Sts')
    DCM_Final_SectionGrid_path = os.path.join(sde_trd_path, 'GISTRD.TRD.DCM_FS_index')

    print("Setting dataset list")
    DCM_datasets = [DCM_path, DCM_SCL_path, DCM_NYMI_path, DCM_ArterialMajorSts_path, DCM_Area_StreetNameChg_path,
                    DCM_Points_StreetNameChg_path, DCM_Lines_StreetNameChg_path, DCM_Final_SectionGrid_path]

    # Define dictionary of new names for exported feature classes
    DCM_new_names = {
        'DCM': 'DCP_TRD_DCM',
        'DCM_SCL': 'DCP_TRD_DCM_StreetCenterLine',
        'DCM_nymi': 'DCP_TRD_DCM_CityMapAlterations',
        'DCM_NmChng_Sts': 'DCP_TRD_DCM_StreetNameChanges_Lines',
        'DCM_Arterials_Mjr_Sts': 'DCP_TRD_DCM_ArterialsMajorStreets',
        'DCP_TRD_StreetNameChanges_Points': 'DCP_TRD_DCM_StreetNameChanges_Points',
        'DCP_TRD_StreetNameChanges_Areas': 'DCP_TRD_DCM_StreetNameChanges_Areas',
        'DCM_FS_index': 'DCP_TRD_DCM_FinalSection_Index'
    }

    # Standardize SDE schema
    dcm_schema_dict = OrderedDict([
        ("Boro_nm", ["Borough", "Borough", "Boro_nm"]),
        ("Type", ["Type", "Type", "Type"]),
        ("Feature_type", ["Feat_Type", "Feature Type", "Feature_type"]),
        ("Feature_subtype", ["Feat_Subtype", "Feature Subtype", "Feature_subtype"]),
        ("Jurisdiction", ["Jurisdiction", "Jurisdiction", "Jurisdiction"]),
        ("Ownership_status", ["Ownership_Status", "Ownership Status", "Ownership_status"]),
        ("Feature_status", ["Feat_Status", "Feature Status", "Feature_status"]),
        ("Record_St", ["Record_ST", "Record Street", "Record_st"]),
        ("Private_St", ["Private_ST", "Private Street", "Private_St"]),
        ("Private_Rd", ["Private_RD", "Private Road", "Private_Rd"]),
        ("Create_DT", ["Create_Date", "Create Date", "Create_DT"]),
        ("Edit_DT", ["Edit_Date", "Edit Date", "Edit_DT"]),
        ("Feature_length", ["Feature_length", "Feature Length", "Feature_length"]),
        ("Length_source", ["Length_source", "Length Source", "Length_source"]),
        ("Lntype", ["Lntype", "Line Type", "lntype"]),
        ("Date", ["Date", "Date", "Date"]),
        ("Notes", ["Notes", "Notes", "Notes"]),
        ("Display", ["Display", "Display", "Display"])

    ])
    dcm_scl_schema_dict = OrderedDict([
        ("Boro_name", ["Borough", "Borough", "Boro_name"]),
        ("DCM_Type", ["Feat_Type", "Feature Type", "DCM_Type"]),
        ("Feature_status", ["Feat_status", "Feature Status", "Feature_status"]),
        ("ROW_Name", ["Street_NM", "Street Name", "Street_Name"]),
        ("HonoraryName", ["HonoraryNM", "Honorary Name", "HonoraryName"]),
        ("Old_ROW_Name", ["Old_ST_NM", "Old Street Name", "OldName"]),
        ("ROW_status", ["Street_status", "Street Status", "Public_access_status"]),
        ("ROW_use", ["Street_use", "Street Use", "ROW_current_use"]),
        ("ROW_purpose", ["Street_purpose", "Street Purpose", "ROW_design_purpose"]),
        ("Streetwidth", ["Streetwidth", "Streetwidth", "Streetwidth"]),
        ("Streetwidth_type", ["Streetwidth_Type", "Streetwidth Type", "Streetwidth_type"]),
        ("Route_type", ["Route_Type", "Route Type", "Route_type"]),
        ("Roadway_type", ["RoadwayType", "Roadway Type", "Roadway_type"]),
        ("LL_type", ["LL_Type", "Local Law Type", "LL_type"]),
        ("Build_status", ["Build_Status", "Build Status", "Build_status"]),
        ("Vested_St", ["Vested_ST", "Vested Street", "Vested_St?"]),
        ("Record_St", ["Record_ST", "Record Street", "Record_St?"]),
        ("Private_St", ["Private_ST", "Private Street", "Private_St"]),
        ("Private_Rd", ["Private_RD", "Private Road", "Private_Rd?"]),
        ("Paper_St", ['Paper_ST', "Paper Street", "Paper_St"]),
        ("Step_stair_St", ["Stair_ST", "Stair Street", "Step_stair_St?"]),
        ("Driveway", ["Driveway", "Driveway", "Driveway?"]),
        ("Old_Rd", ["Old_RD", "Old Road", "Old_Rd?"]),
        ("Orphaned", ["Orphaned", "Orphaned", "Orphaned?"]),
        ("Co_name", ["Co_Name", "Co Name", "Co_name"]),
        ("Fee", ["Fee", "Fee", "Fee"]),
        ("Fee_status", ["Fee_Status", "Fee Status", "Fee_status"]),
        ("Title", ["Title", "City Title", "City_Title?"]),
        ("Title_status", ["Title_status", "BP Title Status", "BP_Title_status"]),
        ("Access_status", ["Access_status", "Access Status", "Access_status (use)"]),
        ("Control", ["City_Control", "City Control", "City_Control (y/n)"]),
        ("Date", ["Date", "Date", "Date"]),
        ("GCL_36", ["GCL_36", 'GCL 36', "GCL36?"]),
        ("CCO", ["CCO_ST", "CCO Street", "CCO?"]),
        ("Marginal_wharf_place", ["Marg_Wharf", "Marginal Wharf Place", "Marginal_wharf_place?"]),
        ("Create_DT", ["Create_DT", "Create Date", "Create_DT"]),
        ("Edit_DT", ['Edit_Date', "Edit Date", "Edit_DT"]),
        ("Source", ["Source", "Source", "Source"]),
        ("Notes", ["Notes", "Notes", "Notes"])
    ])
    nymi_schema_dict = OrderedDict([
        ("BOROCODE", ["Boro_Code", "Borough Code", "BOROCODE"]),
        ("APP_NUM", ["APP_NUM", "Application NUM", "APP_NUM"]),
        ("Project_name", ["Project_NM", "Project Name", "Project_name"]),
        ("Filed_status", ["Filed_Status", "Filed Status", "Filed_status"]),
        ("STATUS", ["Status", "Status", "Status"]),
        ("EFFECTIVE", ["Effect_DT", "Effective Date", "EFFECTIVE"]),
        ("BP_NUM", ["BP_NUM", "BP Number", "BP_NUM"]),
        ("Reso_NUM", ["CC_RES_NUM", "CC Res Number", "Reso_NUM"]),
        ("ZRUPDATE", ["ZR_Update", "ZR Update", "ZRUPDATE"]),
        ("Cert_DT", ["Cert_Date", "Cert Date", "Cert_DT"]),
        ("CHG_TYPE", ["CHG_TYPE", "Change Type", "CHG_TYPE"]),
        ("ALTMAPPDF", ["ALTMAPPDF", "Alt Map PDF", 'ALTMAPPDF']),
        ("Link_address", ["ALTMAPLink", "ALTMAP Link", "Link_address"]),
        ("SCANNED", ["Scanned", "Scanned", "SCANNED"]),
        ("TRACKINGNO", ["Track_NUM", "Tracking Number", "TRACKINGNO"]),
        ("SOURCE", ["Source", "Source", "SOURCE"]),
        ("MAPSERIES", ["Map_Series", "Map Series", "MAPSERIES"]),
        ("MAP_CABINET", ["Map_Cabinet", "Map Cabinet", "MAP_CABINET"]),
        ("MAP_COPIES", ["Map_Copies", "Map Copies", "MAP_COPIES"])
    ])
    area_schema_dict = OrderedDict([
        ("Borough", ["Borough", "Borough", "Borough"]),
        ("Feat_type", ["Feat_Type", "Feature Type", "FeatureType"]),
        ("OfficialName", ["OfficialNM", "Official Street Name", "OfficialName"]),
        ("Hon_NM", ["Honor_Name", "Honorary Name", "HonoraryName"]),
        ("OldName", ["Old_Name", "Old Name", "OldName"]),
        ("CPULPNUM", ["ULURPCPNUM", "ULURP CP Number", "CP_ULURPNumber"]),
        ("IntNum", ["Intro_NUM", "Intro Number", "IntroNumber"]),
        ("Intro_year", ["Intro_Year", "Intro Year", "Intro_year"]),
        ("Int_month", ["IntroMonth", "Intro Month", "Intro_month"]),
        ("Int_day", ["Intro_Day", "Intro Day", "Intro_day"]),
        ("LL_NUM", ["LL_NUM", "Local Law Number", "LocalLawNumber"]),
        ("LL_SEC", ["LL_SEC", "Local Law Section", "LocalLawSection"]),
        ("LL_type", ["LL_Type", "Local Law Type", "Type"]),
        ("LL_Effect", ["LLEffectDT", "LL Effective Date", "LL_effective"]),
        ("LL_Limit", ["LL_Limits", "Local Law Limits", "LL_Limit"]),
        ("LimitsError", ["Limits_ER", "Limits Error", "LimitsError"]),
        ("IntLLNUMSEC", ["INT_LL_SEC", "Intro LL SEC", "Int_LLNum_Sec"]),
        ("CCFileNM", ["CCFileNUM", "CC File Number", "CC_filenum"]),
        ("CCLLNUM", ["CCLLNUM", "CC LL Number", "CC_LL_num"]),
        ("Repeal", ["Repealed", "Repealed", "Repealed"]),
        ("Repeal_DT", ["Repeal_DT", "Repealed Date", "Repealed_dt"]),
        ("Rep_YR_LLSEC", ["RepYRLLSEC", "Repeal Year LL SEC", "Repealed_year_LLsec"]),
        ("Amend", ["Amended", "Amended", "Amended"]),
        ("Amend_DT", ["Amend_DT", "Amended Date", "Amended_dt"]),
        ("Amendment_text", ["Amendt_TXT", "Amendment Text", "Amendment_text"]),
        ("AmendYRLLSEC", ["AMDYRLLSEC", "Amend Year LL SEC", "Amend_year_LLsec"])])
    line_schema_dict = OrderedDict([
        ("Borough", ["Borough", "Borough", "Borough"]),
        ("Feat_type", ["Feat_Type", "Feature Type", "Feature Type"]),
        ("OfficialName", ["OfficialNM", "Official Street Name", "Official Street Name"]),
        ("Hon_NM", ["Honor_Name", "Honorary Name", "Honorary Name"]),
        ("OldName", ["Old_Name", "Old Name", "Old Name"]),
        ("CPULPNUM", ["ULURPCPNUM", "ULURP CP Number", "CP/ULURP#"]),
        ("IntNum", ["Intro_NUM", "Intro Number", "Intro Number"]),
        ("Int_YR", ["Intro_Year", "Intro Year", "Intro Year"]),
        ("Int_month", ["IntroMonth", "Intro Month", "Intro Month"]),
        ("Int_day", ["Intro_Day", "Intro Day", "Intro Day"]),
        ("LL_NUM", ["LL_NUM", "Local Law Number", "Local Law Number"]),
        ("LL_SEC", ["LL_SEC", "Local Law Section", "Local Law Section"]),
        ("LL_type", ["LL_Type", "Local Law Type", "LL_type"]),
        ("LL_Effect", ["LLEffectDT", "LL Effective Date", "LL Effective"]),
        ("LL_Limits", ["LL_Limits", "Local Law Limits", "LL_Limits"]),
        ("LimitsError", ["Limits_ER", "Limits Error", "Limits Error"]),
        ("IntLLNUMSEC", ["INT_LL_SEC", "Intro LL SEC", "Intro/LL#SEC"]),
        ("CCFileNM", ["CCFileNUM", "CC File Number", "CC/File#"]),
        ("CCLLNUM", ["CCLLNUM", "CC LL Number", "CC/LL#"]),
        ("Repeal", ["Repealed", "Repealed", "Repealed?"]),
        ("Repeal_DT", ["Repeal_DT", "Repealed Date", "Repealed Date"]),
        ("Rep_YR_LLSEC", ["RepYRLLSEC", "Repeal Year LL SEC", "Repealed Year/LLSEC"]),
        ("Amend", ["Amended", "Amended", "Amended?"]),
        ("Amend_DT", ["Amend_DT", "Amended Date", "Amended Date"]),
        ("Amendt_TXT", ["Amendt_TXT", "Amendment Text", "Amendment Text"]),
        ("AmendYRLLSEC", ["AMDYRLLSEC", "Amend Year LL SEC", "AmendYear/LLSEC"])])
    pt_schema_dict = OrderedDict([
        ("Borough", ["Borough", "Borough", "Borough"]),
        ("Feat_type", ["Feat_Type", "Feature Type", "FeatureType"]),
        ("OfficialName", ["OfficialNM", "Official Street Name", "OfficialName"]),
        ("Hon_NM", ["Honor_Name", "Honorary Name", "HonoraryName"]),
        ("OldName", ["Old_Name", "Old Name", "OldName"]),
        ("CPULPNUM", ["ULURPCPNUM", "ULURP CP Number", "CP_ULURPNumber"]),
        ("IntNum", ["Intro_NUM", "Intro Number", "IntroNumber"]),
        ("intro_year", ["Intro_Year", "Intro Year", "intro_year"]),
        ("Int_month", ["IntroMonth", "Intro Month", "Intro_month"]),
        ("Int_day", ["Intro_Day", "Intro Day", "Intro_day"]),
        ("LL_NUM", ["LL_NUM", "Local Law Number", "LocalLawNumber"]),
        ("LL_SEC", ["LL_SEC", "Local Law Section", "LocalLawSection"]),
        ("LL_type", ["LL_Type", "Local Law Type", "LL_type"]),
        ("LL_Effect", ["LLEffectDT", "LL Effective Date", "LL_effective"]),
        ("LL_Limits", ["LL_Limits", "Local Law Limits", "LL_Limits"]),
        ("LimitsError", ["Limits_ER", "Limits Error", "LimitsError"]),
        ("IntLLNUMSEC", ["INT_LL_SEC", "Intro LL SEC", "Int_LLNum_sec"]),
        ("CCFileNM", ["CCFileNUM", "CC File Number", "CC_filenum"]),
        ("CCLLNUM", ["CCLLNUM", "CC LL Number", "CC_LL_num"]),
        ("Repeal", ["Repealed", "Repealed", "Repealed"]),
        ("Repeal_DT", ["Repeal_DT", "Repealed Date", "Repealed_date"]),
        ("Rep_YR_LLSEC", ["RepYRLLSEC", "Repeal Year LL SEC", "Repealed_year_LLsec"]),
        ("Amend", ["Amended", "Amended", "Amended"]),
        ("amended_dt", ["Amend_DT", "Amended Date", "amended_dt"]),
        ("Amendment_text", ["Amendt_TXT", "Amendment Text", "Amendment_text"]),
        ("AmendYRLLSEC", ["AMDYRLLSEC", "Amend Year LL SEC", "Amend_year_LLsec"])])
    arterial_schema_dict = OrderedDict([
        ("Boro_nm", ["Borough", "Borough", "Boro_nm"]),
        ("BoroCode", ["Boro_Code", "Borough Code", "BoroCode"]),
        ("Name", ["Route_Name", "Route Name", "Name"]),
        ("Route_type", ["Route_Type", "Route Type", "Route_type"]),
        ("Subtype", ["Route_Sub", "Route Subtype", "SubType"]),
        ("Existing_Proposed", ["Route_Status", "Route Status", "Existing_Proposed"]),
        ("Source", ["Source", "Source", "Source"])
    ])
    fs_grid_schema_dict = OrderedDict([
        ("Boro", ["Borough", "Borough", "Boro"]),
        ("Index_id", ["Index_ID", "Index", "Index"]),
        ("address", ["Address", "File Address", "file_address"]),
        ("latest_date", ["Update_Date", "Update Date", "FS_update_date"]),
        ("Scanned", ["Scanned", "Scanned", "Scanned_y_n"]),
        ("Datum", ["Datum", "Datum", "Datum"]),
        ("Notes", ["Notes", "Notes", "Notes"]),
    ])

    # Disconnect SDE users to remove locks during FC transfer
    def disconnect_disable_sde(TrueOrFalse, sde_path):
        arcpy.env.workspace = sde_path
        arcpy.env.overwriteOutput = True
        if TrueOrFalse.lower() == 'true':
            print("Disconnecting users and blocking sde access until transfer is complete")
            arcpy.AcceptConnections(sde_path, False)
            arcpy.DisconnectUser(sde_path, "ALL")
        else:
            print("Ignoring user/sde disconnect")

    # Create appropraite schemas
    def reorder_rename_export(dataset, dict):
        fms = arcpy.FieldMappings()
        fms.addTable(dataset)
        flds = fms.fieldMappings
        fld_txt = [fld.getInputFieldName(0) for fld in flds]
        new_fms = arcpy.FieldMappings()
        for fldName in dict.keys():
            if fldName in fld_txt:
                print("Hit - {}, {} -> {} | {}".format(dataset, fldName,
                                                       str(dict[fldName][0]), str(dict[fldName][1])))
                fm = arcpy.FieldMap()
                fm.addInputField(dataset, fldName)
                of = fm.outputField
                of.name = str(dict[fldName][0])
                of.alias = str(dict[fldName][1])
                fm.outputField = of
            new_fms.addFieldMap(fm)
            new_string = new_fms.exportToString()
            old_str = '"{}"'.format(dict[fldName][2])
            new_string = new_string.replace(old_str, '"{}"'.format(dict[fldName][1]))
            new_fms.loadFromString(new_string)
        return (new_fms)

    # Define function for exporting to Production SDE
    def export_data_sde(dataset):
        arcpy.env.workspace = meta_path
        arcpy.env.overwriteOutput = True
        dataset_fname = DCM_new_names[dataset.split('.')[-1]].replace('DCP_TRD_', '')
        tree = ET.parse(os.path.join(template_path, '{}.xml'.format(dataset_fname)))
        root = tree.getroot()
        for summary_fgdc in root.iter('purpose'):
            summary_fgdc.text += ' Dataset last updated: {}.'.format(pub_longform)
        for summary_fgdc in root.iter('idPurp'):
            summary_fgdc.text += " Dataset last updated: {}.".format(pub_longform)
        for pubdate in root.iter("pubDate"):
            pubdate.text = publication_date
        for pubdate in root.iter("pubdate"):
            pubdate.text = publication_date
        print("Writing updated metadata to {}".format(dataset_fname))
        tree.write(os.path.join(meta_path, '{}_published.xml'.format(dataset_fname)))
        print("Exporting metadata for {} with geoprocessing history removed".format(DCM_new_names[dataset.split('.')[-1]]))
        arcpy.XSLTransform_conversion(os.path.join(meta_path, '{}_published.xml'.format(dataset_fname)),
                                      xslt_geoproc_hist,
                                      os.path.join(meta_path, '{}_working.xml'.format(dataset_fname)))
        print("Metadata for {} exported with geoprocessing history removed".format(dataset_fname))
        print("Exporting metadata for {} with local storage removed".format(dataset_fname))
        arcpy.XSLTransform_conversion(os.path.join(meta_path, '{}_working.xml'.format(dataset_fname)),
                                      xslt_lcl_strg,
                                      os.path.join(meta_path, 'SDE_{}.xml'.format(dataset_fname)))
        print("Deleting intermediary working stand-alone xml metadata")
        arcpy.Delete_management(os.path.join(meta_path, '{}_unmodified.xml'.format(dataset_fname)))
        arcpy.Delete_management(os.path.join(meta_path, '{}_working.xml'.format(dataset_fname)))
        arcpy.Delete_management(os.path.join(meta_path, '{}_published.xml'.format(dataset_fname)))
        print("Intermediary working stand-alone xml deleted")

        if dataset == DCM_path:
            print("Outputting DCM")
            new_fms = reorder_rename_export(dataset, dcm_schema_dict)
            res = arcpy.FeatureClassToFeatureClass_conversion(dataset, sde_prod_path,
                                                              "DCP_TRD_{}".format(dataset_fname), field_mapping=new_fms) # where_clause=sql_expression,
            del res
        if dataset == DCM_SCL_path:
            print("Outputting DCM_SCL")
            new_fms = reorder_rename_export(dataset, dcm_scl_schema_dict)
            res = arcpy.FeatureClassToFeatureClass_conversion(dataset, sde_prod_path,
                                                              "DCP_TRD_{}".format(dataset_fname), field_mapping=new_fms)
            del res
        if dataset == DCM_NYMI_path:
            print("Outputting NYMI")
            new_fms = reorder_rename_export(dataset, nymi_schema_dict)
            res = arcpy.FeatureClassToFeatureClass_conversion(dataset, sde_prod_path,
                                                              "DCP_TRD_{}".format(dataset_fname), field_mapping=new_fms)
            del res
        if dataset == DCM_Area_StreetNameChg_path:
            print("Outputting StreetNameChg Areas")
            new_fms = reorder_rename_export(dataset, area_schema_dict)
            res = arcpy.FeatureClassToFeatureClass_conversion(dataset, sde_prod_path,
                                                              "DCP_TRD_{}".format(dataset_fname), field_mapping=new_fms)
            del res
        if dataset == DCM_Lines_StreetNameChg_path:
            print("Outputting StreetNameChg Lines")
            new_fms = reorder_rename_export(dataset, line_schema_dict)
            res = arcpy.FeatureClassToFeatureClass_conversion(dataset, sde_prod_path,
                                                              "DCP_TRD_{}".format(dataset_fname), field_mapping=new_fms)
            del res
        if dataset == DCM_Points_StreetNameChg_path:
            print("Outputting StreetNameChg Points")
            new_fms = reorder_rename_export(dataset, pt_schema_dict)
            res = arcpy.FeatureClassToFeatureClass_conversion(dataset, sde_prod_path,
                                                              "DCP_TRD_{}".format(dataset_fname), field_mapping=new_fms)
            del res
        if dataset == DCM_ArterialMajorSts_path:
            print("Outputting Arterial Major Streets")
            new_fms = reorder_rename_export(dataset, arterial_schema_dict)
            res = arcpy.FeatureClassToFeatureClass_conversion(dataset, sde_prod_path,
                                                              "DCP_TRD_{}".format(dataset_fname), field_mapping=new_fms)
            del res
        if dataset == DCM_Final_SectionGrid_path:
            print("Outputting Final Section Grid")
            new_fms = reorder_rename_export(dataset, fs_grid_schema_dict)
            res = arcpy.FeatureClassToFeatureClass_conversion(dataset, sde_prod_path,
                                                              "DCP_TRD_{}".format(dataset_fname), field_mapping=new_fms)
            del res
        print("Re-importing metadata to {} feature class".format(dataset_fname))
        print("Re-importing metadata".format(dataset_fname))
        arcpy.MetadataImporter_conversion(os.path.join(meta_path, "SDE_{}.xml".format(dataset_fname)),
                                          os.path.join(sde_prod_path, "DCP_TRD_{}".format(dataset_fname)))

    print("Exporting feature classes to PROD SDE from TRD SDE")

    # Allow overwrite and disconnect active users who may be locking desired SDE Feature Classes
    disconnect_disable_sde(disconnect_sde, sde_prod_path)

    arcpy.env.workspace = sde_prod_path
    arcpy.env.overwriteOutput = True

    export_data_sde(DCM_path)
    time.sleep(5)
    export_data_sde(DCM_SCL_path)
    time.sleep(5)
    export_data_sde(DCM_NYMI_path)
    time.sleep(5)
    export_data_sde(DCM_ArterialMajorSts_path)
    time.sleep(5)
    export_data_sde(DCM_Area_StreetNameChg_path)
    time.sleep(5)
    export_data_sde(DCM_Points_StreetNameChg_path)
    time.sleep(5)
    export_data_sde(DCM_Lines_StreetNameChg_path)
    time.sleep(5)
    export_data_sde(DCM_Final_SectionGrid_path)

    # Define function for replacing xmls
    def distribute_xmls(xml, lyr_tmp_dir, lyr_rep_path, lyr_rep_files):
        xml_title = xml.split('.')[0]
        if xml_title in lyr_rep_files:
            tree = ET.parse(os.path.join(lyr_tmp_dir, '{}.lyr.xml'.format(xml_title)))
            root = tree.getroot()
            for summary_fgdc in root.iter('idPurp'):
                summary_fgdc.text += ' Dataset last updated: {}.'.format(pub_longform)
            for summary_fgdc in root.iter('purpose'):
                summary_fgdc.text += ' Dataset last updated: {}.'.format(pub_longform)
            for pubdate in root.iter("pubDate"):
                pubdate.text = publication_date
            for pubdate in root.iter("pubdate"):
                pubdate.text = publication_date

            print("Renaming old xmls")
            os.rename(os.path.join(lyr_rep_path, '{}.lyr.xml'.format(xml_title)),
                      os.path.join(lyr_rep_path, '{}_old.lyr.xml'.format(xml_title)))
            print("Writing updated metadata to {}".format(xml_title))
            tree.write(os.path.join(lyr_rep_path, '{}_published.lyr.xml'.format(xml_title)))
            print("Exporting metadata for {} with geoprocessing history removed".format(xml_title))
            arcpy.XSLTransform_conversion(os.path.join(lyr_rep_path, '{}_published.lyr.xml'.format(xml_title)),
                                          xslt_geoproc_hist,
                                          os.path.join(lyr_rep_path, '{}_working.lyr.xml'.format(xml_title)))
            print("Metadata for {} exported with geoprocessing history removed".format(xml_title))
            print("Exporting metadata for {} with local storage removed".format(xml_title))
            arcpy.XSLTransform_conversion(os.path.join(lyr_rep_path, '{}_working.lyr.xml'.format(xml_title)),
                                          xslt_lcl_strg,
                                          os.path.join(lyr_rep_path, '{}.lyr.xml'.format(xml_title)))
            print("Deleting intermediary working stand-alone xml metadata")
            arcpy.Delete_management(os.path.join(lyr_rep_path, '{}_working.lyr.xml'.format(xml_title)))
            arcpy.Delete_management(os.path.join(lyr_rep_path, '{}_published.lyr.xml'.format(xml_title)))
            arcpy.Delete_management(os.path.join(lyr_rep_path, '{}_old.lyr.xml'.format(xml_title)))


    # Reallow SDE connectivity regardless of selection.
    arcpy.AcceptConnections(sde_prod_path, True)

    # Generate lyr xmls with updated date information
    streets_lyr_dcm_path = os.path.join(lyr_dcm_path, 'Streets Application')
    lyr_xml_path = os.path.join(template_path, 'lyr_xmls')
    streetsapp_lyr_xml_path = os.path.join(lyr_xml_path, 'streets app layer')

    lyr_dcm_files = [lyr.split('.')[0] for lyr in os.listdir(lyr_dcm_path) if lyr.endswith('.xml')]
    streetsapp_lyr_files = [lyr.split('.')[0] for lyr in os.listdir(streets_lyr_dcm_path) if lyr.endswith('.xml')]

    for xml in os.listdir(lyr_xml_path):
        distribute_xmls(xml, lyr_xml_path, lyr_dcm_path, lyr_dcm_files)
    for file in os.listdir(lyr_xml_path):
        if 'lyr_xslttransformation' in file or file.endswith('.log'):
            arcpy.Delete_management(os.path.join(lyr_xml_path, file))

    for xml in os.listdir(streetsapp_lyr_xml_path):
        distribute_xmls(xml, streetsapp_lyr_xml_path, streets_lyr_dcm_path, streetsapp_lyr_files)
    for file in os.listdir(streetsapp_lyr_xml_path):
        if 'lyr_xslttransformation' in file or file.endswith('.log'):
            arcpy.Delete_management(os.path.join(streetsapp_lyr_xml_path, file))

    print("Done")

    EndTime = datetime.now().replace(microsecond=0)
    print("Script runtime: {}".format(EndTime - StartTime))
    log.write(str(StartTime) + "\t" + str(EndTime) + "\t" + str(EndTime - StartTime) + "\n")
    log.close()

except:
    # Reconnect SDE users
    arcpy.AcceptConnections(sde_prod_path, True)
    tb = sys.exc_info()[2]
    tbinfo = traceback.format_tb(tb)[0]

    pymsg = "PYTHON ERRORS:\nTraceback Info:\n" + tbinfo + "\nError Info:\n" + str(sys.exc_info()[1])
    msgs = "ArcPy ERRORS:\n" + arcpy.GetMessages() + "\n"

    print(pymsg)
    print(msgs)

    log.write("" + pymsg + "\n")
    log.write("" + msgs + "")
    log.write("\n")
    log.close()