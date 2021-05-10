import arcpy, os, zipfile, traceback, shutil, ConfigParser, sys, xml.etree.ElementTree as ET
from datetime import datetime
from collections import OrderedDict

try:

    StartTime = datetime.now().replace(microsecond=0)

    # Assign config path
    root_path = os.path.dirname(os.path.dirname(__file__))
    ini_path = os.path.join(root_path, 'ini', 'dcm_config.ini')
    
    # Set configuration file path
    config = ConfigParser.ConfigParser()
    config.read(ini_path)

    # Set log file path
    log_path = config.get("PATHS", "log_path")
    log = open(os.path.join(log_path, "DCM_BP_distribution_log.txt"), "a")

    # Set necessary connection and directory paths and date variable
    print("Setting output directory paths")


    publication_date = config.get("VARS", "publication_date")
    pubdate = datetime.strptime(publication_date, '%Y%m%d')
    pub_yr = str(pubdate.year)
    pub_longform = pubdate.strftime("%B %d, %Y")
    translator = config.get("PATHS", "translator")
    # xslt_html = config.get("PATHS", "xslt_html")
    xslt_lcl_strg = config.get("PATHS", "xslt_lcl_strg")
    xslt_geoproc_hist = config.get("PATHS", "xslt_geoproc_hist")
    sde_trd_path = config.get("PATHS", "sde_trd_path")
    sde_prod_path = config.get("PATHS", "sde_prod_path")
    sde_dzm_path = os.path.join(sde_trd_path, "GISTRD.TRD.Digital_Zoning_Map")
    lyr_dcm_path = config.get("PATHS", "lyr_dcm_path")
    localtemp_path = config.get("PATHS", "export_path")
    current_export_path = os.path.join(localtemp_path, pub_yr, publication_date)
    bytesprod_path = config.get("PATHS", "production_export_path")
    current_prod_export_path = os.path.join(bytesprod_path, pub_yr, publication_date)
    template_path = config.get("PATHS", "template_path")
    city_map_alteration_path = config.get("PATHS", "city_map_alteration_path")
    meta_path = os.path.join(current_export_path, "meta")
    fgdb_path = os.path.join(current_export_path, "fgdb")
    shp_path = os.path.join(current_export_path, "shp")
    web_path = os.path.join(current_export_path, "web")

    # Generate directories if they do not already exist
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

    if os.path.exists(fgdb_path):
        print("File Geodatabase path exists. Skipping")
    else:
        os.mkdir(fgdb_path)

    if os.path.exists(shp_path):
        print("Shapefile path exists. Skipping")
    else:
        os.mkdir(shp_path)

    if os.path.exists(web_path):
        print("Web path exists. Skipping")
    else:
        os.mkdir(web_path)

    if os.path.exists(meta_path):
        print("Meta path exists. Skipping")
    else:
        os.mkdir(meta_path)
    if os.path.exists(os.path.join(fgdb_path, 'DCM.gdb')):
        print("DCM.gdb exists. Skipping")
    else:
        arcpy.CreateFileGDB_management(fgdb_path, 'DCM.gdb')

    # Define TRD SDE feature class paths
    print("Setting input data paths")
    DCM_path = os.path.join(sde_dzm_path, "GISTRD.TRD.DCM")
    DCM_SCL_path = os.path.join(sde_dzm_path, "GISTRD.TRD.DCM_SCL")
    DCM_NYMI_path = os.path.join(sde_dzm_path, "GISTRD.TRD.DCM_nymi")
    DCM_ArterialMajorSts_path = os.path.join(sde_dzm_path, "GISTRD.TRD.DCM_Arterials_Mjr_Sts")
    DCM_Area_StreetNameChg_path = os.path.join(sde_dzm_path, "GISTRD.TRD.DCP_TRD_StreetNameChanges_Areas")
    DCM_Points_StreetNameChg_path = os.path.join(sde_dzm_path, "GISTRD.TRD.DCP_TRD_StreetNameChanges_Points")
    DCM_Lines_StreetNameChg_path = os.path.join(sde_dzm_path, "GISTRD.TRD.DCM_NmChng_Sts")

    # DCM_Final_SectionGrid_path = os.path.join(sde_trd_path, "GISTRD.TRD.DCM_FS_index")
    print("Setting dataset list")
    DCM_datasets = [DCM_path, DCM_SCL_path, DCM_NYMI_path, DCM_ArterialMajorSts_path, DCM_Area_StreetNameChg_path,
                    DCM_Points_StreetNameChg_path, DCM_Lines_StreetNameChg_path]

    # Define fields to be retained for datasets with restricted schemas
    print("Define retained fields")
    shp_retain = ["FID", "Shape", "Shape_STAr", "Shape_STLe"]
    fgdb_retain = ["OBJECTID", "Shape", "Shape_Length", "Shape_Area", ]
    sde_retain = ["OBJECTID", "Shape", "Shape.STLength()", "Shape.STArea()"]
    DCM_shp_retain = ["Feat_Type", "Borough", "Jurisdicti", "Record_ST", "Edit_Date"]
    DCM_fgdb_retain = ["Borough", "Feat_Type", "Jurisdiction", "Record_ST", "Edit_Date"]
    DCM_SCL_shp_retain = ["Old_ST_NM", "Route_Type", "Streetwidt", "Street_NM", "HonoraryNM", "RoadwayTyp", "Borough",
                          "Feat_statu", "Feat_Type", "Build_Stat", "Marg_Wharf", "Stair_ST", "CCO_ST", "Edit_Date",
                          "Paper_ST", "Record_ST"]
    DCM_SCL_fgdb_retain = ["Old_ST_NM", "Route_Type", "Streetwidth", "Street_NM", "HonoraryNM", "RoadwayType",
                           "Borough", "Feat_status", "Feat_Type", "Build_Status", "Marg_Wharf", "Stair_ST", "CCO_ST",
                           "Edit_Date", "Paper_ST", "Record_ST"]
    DCM_AlterationMaps_shp_retain = ["ALTMAPPDF", "Cmplt_status", "Inset", "Boro_Code", "APP_NUM", "Project_name",
                                     "Filed_Stat", "Status", "BP_NUM", "CC_RES_NUM", "ZR_Update", "Cert_Date",
                                     "CHG_TYPE", "Scanned", "Project_NM", "Effect_DT", "ALTMAPLink", "Track_NUM",
                                     "Source", "Map_Series", "Map_Cabine", "Map_Copies"]
    DCM_AlterationMaps_fgdb_retain = ["ALTMAPPDF", "Cmplt_status", "Inset", "Boro_Code", "APP_NUM", "Project_name",
                                      "Filed_Status", "Status", "BP_NUM", "CC_RES_NUM", "ZR_Update", "Cert_Date",
                                      "CHG_TYPE", "Scanned", "Project_NM", "Effect_DT", "ALTMAPLink", "Track_NUM",
                                      "Source", "Map_Series", "Map_Cabinet", "Map_Copies"]
    DCM_Street_nm_chg_retain = ["Borough", "Feat_Type", "OfficialNM", "Honor_Name", "Old_Name", "ULURPCPNUM",
                                "Intro_NUM", "Intro_Year", "IntroMonth", "Intro_Day",  "LL_NUM", "LL_SEC", "LL_Type",
                                "LLEffectDT", "LL_Limits", "Limits_ER", "Repealed", "Repeal_DT", "Amended", "Amend_DT",
                                "Amendt_TXT", "AMDYRLLSEC"]

    # Define dictionary of new names for exported feature classes
    DCM_new_names = {
        "DCM": "DCP_TRD_DCM",
        "DCM_SCL": "DCP_TRD_DCM_StreetCenterLine",
        "DCM_nymi": "DCP_TRD_DCM_CityMapAlterations",
        "DCM_NmChng_Sts": "DCP_TRD_DCM_StreetNameChanges_Lines",
        "DCM_Arterials_Mjr_Sts": "DCP_TRD_DCM_ArterialsMajorStreets",
        "DCP_TRD_StreetNameChanges_Points": "DCP_TRD_DCM_StreetNameChanges_Points",
        "DCP_TRD_StreetNameChanges_Areas": "DCP_TRD_DCM_StreetNameChanges_Areas",
    }

    # DCM_html_names = {
    #     "DCM": "dcm",
    #     "DCM_StreetCenterLine": "dcm_street_centerline",
    #     "DCM_CityMapAlterations": "dcm_city_map_alterations",
    #     "DCM_StreetNameChanges_Lines": "dcm_street_name_changes_lines",
    #     "DCM_ArterialsMajorStreets": "dcm_arterials_major_streets",
    #     "DCM_StreetNameChanges_Points": "dcm_street_name_changes_points",
    #     "DCM_StreetNameChanges_Areas": "dcm_street_name_changes_areas",
    # }

    # Standardize fgdb and shapefile schema
    dcm_schema_dict = OrderedDict([
        ("Boro_nm", ["Borough", "Borough", "Boro_nm"]),
        ("Feature_type", ["Feat_Type", "Feature Type", "Feature_type"]),
        ("Jurisdiction", ["Jurisdiction", "Jurisdiction", "Jurisdiction"]),
        ("Record_St", ["Record_ST", "Record Street", "Record_st"]),
        ("Edit_DT", ["Edit_Date", "Edit Date", "Edit_DT"])
    ])
    dcm_scl_schema_dict = OrderedDict([
        ("Boro_name", ["Borough", "Borough", "Boro_name"]),
        ("DCM_Type", ["Feat_Type", "Feature Type", "DCM_Type"]),
        ("Feature_status", ["Feat_status", "Feature Status", "Feature_status"]),
        ("ROW_Name", ["Street_NM", "Street Name", "Street_Name"]),
        ("HonoraryName", ["HonoraryNM", "Honorary Name", "HonoraryName"]),
        ("Old_ROW_Name", ["Old_ST_NM", "Old Street Name", "OldName"]),
        ("Streetwidth", ["Streetwidth", "Streetwidth", "Streetwidth"]),
        ("Route_type", ["Route_Type", "Route Type", "Route_type"]),
        ("Roadway_type", ["RoadwayType", "Roadway Type", "Roadway_type"]),
        ("Build_status", ["Build_Status", "Build Status", "Build_status"]),
        ("Record_St", ["Record_ST", "Record Street", "Record_St?"]),
        ("Paper_St", ['Paper_ST', "Paper Street", "Paper_St"]),
        ("Step_stair_St", ["Stair_ST", "Stair Street", "Step_stair_St?"]),
        ("CCO", ["CCO_ST", "CCO Street", "CCO?"]),
        ("Marginal_wharf_place", ["Marg_Wharf", "Marginal Wharf Place", "Marginal_wharf_place?"]),
        ("Edit_DT", ['Edit_Date', "Edit Date", "Edit_DT"]),
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
        ("Subtype", ["Route_Sub", "Route Subtype", "Subtype"]),
        ("Existing_Proposed", ["Route_Status", "Route Status", "Existing_Proposed"])
    ])


    def reorder_rename_export(dataset, dictionary, out_type):
        fms = arcpy.FieldMappings()
        print("Adding table field map")
        fms.addTable(dataset)
        flds = fms.fieldMappings
        fld_txt = [fld.getInputFieldName(0) for fld in flds]
        new_fms = arcpy.FieldMappings()
        print("Looping through fields")
        for fldName in dictionary.keys():
            if fldName in fld_txt:
                print("Hit - {}, {} -> {} | {}".format(dataset, fldName,
                                                       str(dictionary[fldName][0]), str(dictionary[fldName][1])))
                fm = arcpy.FieldMap()
                fm.addInputField(dataset, fldName)
                of = fm.outputField
                of.name = str(dictionary[fldName][0])
                of.alias = str(dictionary[fldName][1])
                fm.outputField = of
            new_fms.addFieldMap(fm)
            if out_type == "fgdb":
                new_string = new_fms.exportToString()
                old_str = '"{}"'.format(dictionary[fldName][2])
                new_string = new_string.replace(old_str, '"{}"'.format(dictionary[fldName][1]))
                new_fms.loadFromString(new_string)
        return new_fms


    def export_data_retain_fields(dataset, output_directory, dataset_retain_list, out_type):
        arcpy.env.workspace = output_directory
        arcpy.env.overwriteOutput = True
        print("Exporting {} to {} directory".format(dataset, output_directory))
        dataset_fname = DCM_new_names[dataset.split(".")[-1]].replace("DCP_TRD_", "")
        tree = ET.parse(os.path.join(template_path, "{}.xml".format(dataset_fname)))
        root = tree.getroot()
        for summary_fgdc in root.iter("purpose"):
            summary_fgdc.text += " Dataset last updated: {}.".format(pub_longform)
        for summary_fgdc in root.iter('idPurp'):
            summary_fgdc.text += " Dataset last updated: {}.".format(pub_longform)
        for pubdate in root.iter("pubDate"):
            pubdate.text = publication_date
        for pubdate in root.iter("pubdate"):
            pubdate.text = publication_date
        print("Writing updated metadata to {}".format(dataset_fname))
        tree.write(os.path.join(meta_path, "BP_{}.xml".format(dataset_fname)))
        if dataset == DCM_path:
            print("Outputting DCM")
            type_delim = arcpy.AddFieldDelimiters(dataset, "Type")
            feature_type_delim = arcpy.AddFieldDelimiters(dataset, "Feature_type")
            sql_expression = """{0} <> '{1}' AND {2} <> '{3}'""".format(type_delim, "Former_ROW", feature_type_delim,
                                                                        "Demapped_St")
            new_fms = reorder_rename_export(dataset, dcm_schema_dict, out_type)
            res = arcpy.FeatureClassToFeatureClass_conversion(dataset, output_directory, dataset_fname,
                                                              where_clause=sql_expression, field_mapping=new_fms)
            del res
        if dataset == DCM_SCL_path:
            print("Outputting DCM_SCL")
            route_type_delim = arcpy.AddFieldDelimiters(dataset, "Route_type")
            feature_status_delim = arcpy.AddFieldDelimiters(dataset, "Feature_status")
            sql_expression = """{0} <> '{1}' AND {2} <> '{3}'""".format(route_type_delim, "Former_ROW",
                                                                        feature_status_delim, "Demapped")
            new_fms = reorder_rename_export(dataset, dcm_scl_schema_dict, out_type)
            res = arcpy.FeatureClassToFeatureClass_conversion(dataset, output_directory, dataset_fname,
                                                              where_clause=sql_expression, field_mapping=new_fms)
            del res
        if dataset == DCM_NYMI_path:
            print("Outputting NYMI")
            status_delim = arcpy.AddFieldDelimiters(dataset, "STATUS")
            sql_expression = """{0} = 13 OR {0} = 14""".format(status_delim)
            new_fms = reorder_rename_export(dataset, nymi_schema_dict, out_type)
            res = arcpy.FeatureClassToFeatureClass_conversion(dataset, output_directory, dataset_fname,
                                                              where_clause=sql_expression, field_mapping=new_fms)
            del res
            if out_type == "fgdb":
                with arcpy.da.UpdateCursor(os.path.join(output_directory, dataset_fname), "ALTMAPLink") as cursor:
                    for row in cursor:
                        if row[0] is not None:
                            print("Old - {}".format(row[0]))
                            row[0] = row[0].replace(city_map_alteration_path,
                                                    r"https://nycdcp-dcm-alteration-maps.nyc3.digitaloceanspaces.com/")
                            print("New - {}".format(row[0]))
                            cursor.updateRow(row)
                        del row
                    del cursor
            if out_type == "shapefile":
                with arcpy.da.UpdateCursor(os.path.join(output_directory,
                                                        "{}.shp".format(dataset_fname)), "ALTMAPLink") as cursor:
                    for row in cursor:
                        if row[0] is not None:
                            print("Old - {}".format(row[0]))
                            row[0] = row[0].replace(city_map_alteration_path,
                                                    r" https://nycdcp-dcm-alteration-maps.nyc3.digitaloceanspaces.com/")
                            print("New - {}".format(row[0]))
                            cursor.updateRow(row)
                        del row
                    del cursor
        if dataset == DCM_Area_StreetNameChg_path:
            print("Outputting StreetNameChg Areas")
            new_fms = reorder_rename_export(dataset, area_schema_dict, out_type)
            res = arcpy.FeatureClassToFeatureClass_conversion(dataset, output_directory, dataset_fname,
                                                              field_mapping=new_fms)
            del res
        if dataset == DCM_Lines_StreetNameChg_path:
            print("Outputting StreetNameChg Lines")
            new_fms = reorder_rename_export(dataset, line_schema_dict, out_type)
            res = arcpy.FeatureClassToFeatureClass_conversion(dataset, output_directory, dataset_fname,
                                                              field_mapping=new_fms)
            del res
        if dataset == DCM_Points_StreetNameChg_path:
            print("Outputting StreetNameChg Points")
            new_fms = reorder_rename_export(dataset, pt_schema_dict, out_type)
            res = arcpy.FeatureClassToFeatureClass_conversion(dataset, output_directory, dataset_fname,
                                                              field_mapping=new_fms)
            del res
        if dataset == DCM_ArterialMajorSts_path:
            print("Outputting Arterial Major Streets")
            new_fms = reorder_rename_export(dataset, arterial_schema_dict, out_type)
            res = arcpy.FeatureClassToFeatureClass_conversion(dataset, output_directory, dataset_fname,
                                                              field_mapping=new_fms)
            del res
        # if dataset == DCM_Final_SectionGrid_path:
        #     print("Outputting Final Section Grid")
        #     res = arcpy.FeatureClassToFeatureClass_conversion(dataset, output_directory, dataset_fname)
        #     del res
        #     res = arcpy.RepairGeometry_management(os.path.join(output_directory, dataset_fname))
        #     del res

        if out_type == "shapefile":
            for field in arcpy.ListFields(dataset_fname + ".shp"):
                if field.name not in dataset_retain_list and dataset_retain_list is not "None" \
                        and field.name not in shp_retain:
                    print("Dropping {} from {}".format(field.name, os.path.join(output_directory,
                                                                                "{}".format(dataset_fname))))
                    res = arcpy.DeleteField_management("{}.shp".format(dataset_fname), field.name)
                    del res
            arcpy.env.workspace = meta_path
            arcpy.env.overwriteOutput = True
            print("Re-importing metadata to {} shapefile".format(dataset_fname))
            print("Re-importing metadata".format(dataset_fname))
            res = arcpy.RepairGeometry_management(os.path.join(output_directory, "{}.shp".format(dataset_fname)))
            del res
            arcpy.MetadataImporter_conversion(os.path.join(meta_path, "BP_{}.xml".format(dataset_fname)),
                                              os.path.join(output_directory, "{}.shp".format(dataset_fname)))
        if out_type == "fgdb":
            for field in arcpy.ListFields(dataset_fname):
                if field.name not in dataset_retain_list and dataset_retain_list is not "None"\
                        and field.name not in fgdb_retain:
                    print("Dropping {} from {}".format(field.name, os.path.join(output_directory,
                                                                                "{}".format(dataset_fname))))
                    res = arcpy.DeleteField_management("{}".format(dataset_fname), field.name)
                    del res
            arcpy.env.workspace = meta_path
            arcpy.env.overwriteOutput = True
            print("Re-importing metadata to {} feature class".format(dataset_fname))
            print("Re-importing metadata".format(dataset_fname))
            res = arcpy.RepairGeometry_management(os.path.join(output_directory, dataset_fname))
            del res
            res = arcpy.Copy_management(os.path.join(output_directory, dataset_fname),
                                        os.path.join(sde_prod_path, "AGO_{}".format(dataset_fname)))
            del res
            arcpy.MetadataImporter_conversion(os.path.join(meta_path, "BP_{}.xml".format(dataset_fname)),
                                              os.path.join(output_directory, dataset_fname))

    print("Exporting shapefiles to shp directory")

    # Export shapefiles
    export_data_retain_fields(DCM_path, shp_path, DCM_shp_retain, "shapefile")
    export_data_retain_fields(DCM_SCL_path, shp_path, DCM_SCL_shp_retain, "shapefile")
    export_data_retain_fields(DCM_NYMI_path, shp_path, DCM_AlterationMaps_shp_retain, "shapefile")
    export_data_retain_fields(DCM_ArterialMajorSts_path, shp_path, "None", "shapefile")
    export_data_retain_fields(DCM_Area_StreetNameChg_path, shp_path, DCM_Street_nm_chg_retain, "shapefile")
    export_data_retain_fields(DCM_Points_StreetNameChg_path, shp_path, DCM_Street_nm_chg_retain, "shapefile")
    export_data_retain_fields(DCM_Lines_StreetNameChg_path, shp_path, DCM_Street_nm_chg_retain, "shapefile")
    #export_data_retain_fields(DCM_Final_SectionGrid_path, shp_path, "None", "shapefile") -- Not released to public

    print("Exporting feature classes to fgdb directory")

    # Export feature classes
    export_data_retain_fields(DCM_path, os.path.join(fgdb_path, "DCM.gdb"), DCM_fgdb_retain, "fgdb")
    export_data_retain_fields(DCM_SCL_path, os.path.join(fgdb_path, "DCM.gdb"), DCM_SCL_fgdb_retain, "fgdb")
    export_data_retain_fields(DCM_NYMI_path, os.path.join(fgdb_path, "DCM.gdb"), DCM_AlterationMaps_fgdb_retain, "fgdb")
    export_data_retain_fields(DCM_ArterialMajorSts_path, os.path.join(fgdb_path, "DCM.gdb"), "None", "fgdb")
    export_data_retain_fields(DCM_Area_StreetNameChg_path, os.path.join(fgdb_path, "DCM.gdb"),
                              DCM_Street_nm_chg_retain, "fgdb")
    export_data_retain_fields(DCM_Points_StreetNameChg_path, os.path.join(fgdb_path, "DCM.gdb"),
                              DCM_Street_nm_chg_retain, "fgdb")
    export_data_retain_fields(DCM_Lines_StreetNameChg_path, os.path.join(fgdb_path, "DCM.gdb"),
                              DCM_Street_nm_chg_retain, "fgdb")
    #export_data_retain_fields(DCM_Final_SectionGrid_path, os.path.join(fgdb_path, "DCM.gdb"), "None", "fgdb")  -- Not released to public


    def zip_files(path, format):
        print("Zipping {}".format(path))
        if format == 'shapefile':
            arcpy.env.workspace = path
            with zipfile.ZipFile(os.path.join(web_path, "DCM_{}shp.zip".format(publication_date)), "w",
                                 zipfile.ZIP_DEFLATED) as zipobj:
                for f in os.listdir(path):
                    print("Zipping {} to web directory".format(f))
                    os.chdir(path)
                    zipobj.write(f)
            zipobj.close()
        if format == 'fgdb':
            arcpy.env.workspace = path
            gdb_path = os.path.join(path, 'DCM.gdb')
            with zipfile.ZipFile(os.path.join(web_path, "DCM_{}fgdb.zip".format(publication_date)), "w", zipfile.ZIP_DEFLATED) as zipobj:
                for root, dirs, files in os.walk(fgdb_path):
                    if root == gdb_path:
                        for f in files:
                            if not f.endswith('lock'):
                                os.chdir(root)
                                zipobj.write(f, r"DCM.gdb\{}".format(f), zipfile.ZIP_DEFLATED)


    print("Zipping files")
    zip_files(shp_path, 'shapefile')
    zip_files(fgdb_path, 'fgdb')

    # # Move html metadata files to web folder
    #
    # arcpy.env.workspace = web_path
    # arcpy.env.overwriteOutput = True
    # for file in os.listdir(meta_path):
    #     if file.endswith("html"):
    #         print("Moving HTML files to web folder")
    #         shutil.copyfile(os.path.join(meta_path, file),
    #                         os.path.join(web_path, file))

    # Export local temp directory to BytesProduction directory

    if os.path.exists(current_export_path) and os.path.exists(current_prod_export_path):
        print("{} path exists. Please temp folder to BytesProduction directory manually".format(current_export_path))
    if os.path.exists(current_export_path) and not os.path.exists(current_prod_export_path):
        print("Copying {} to {}".format(current_export_path, current_prod_export_path))
        arcpy.Copy_management(current_export_path, current_prod_export_path)
    else:
        print("{} doesn't exist. Check logs to ensure successful copy".format(current_export_path))

    print("Done")

    EndTime = datetime.now().replace(microsecond=0)
    print("Script runtime: {}".format(EndTime - StartTime))
    log.write(str(StartTime) + "\t" + str(EndTime) + "\t" + str(EndTime - StartTime) + "\n")
    log.close()

except:
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