# Tit
play

#########
gc()











###################################################################
ExecuteSQL <- dget("extra-ExecuteSQL.r")
Property_NOP_pipeline <- dget("Property_NOP_pipeline.r")
Target_allocation_cleaning_pipeline <- dget("Pre_target_allocation_pipeline.r")
Numeric_transformation_pipeline <- dget("Numerical_transform_pipeline.r")
Target_allocation_cleaning_targets_pipeline <- dget("Target_allocation_cleaning_targets_pipeline.r")
TTC_target_allocation_pipeline <- dget("TTC_target_allocation_pipeline.r")
Floor_allocation_pipeline <- dget("Floor_allocation_pipeline.r")
Data_split_pipeline <- dget("Data_split_pipeline_column.r")
Calibrate_alpha_pipeline <- dget("Calibrate_alpha_pipeline_optim.r")
Distinct_knid_pipeline <- dget("Distinct_knid_pipeline.r")
Target_grouping <- dget("Target_grouping.r")
Regulatory_PD <- dget("Regulatory_PD.r")
Alocate_alphas <- dget("Alocate_alphas.r")
PD_masterscale <- dget("PD_masterscale.r")
Residensland_pipeline <- dget("Residensland_pipeline.r")
PD_push_allocation_pipeline <- dget("PD_push_allocation_pipeline.r")
RD_fix <- dget("RD_fix.r")
FIRB_AIRB_flag <- dget("FIRB_AIRB_flag.r")
Trim <- dget("Trim.r")

#Calculate_r <- dget("Calculate_r.r")
#Calculate_b <- dget("Calculate_b.r")
#Calculate_RWA <- dget("Calculate_RWA.r")


########Set additional variables for data extraction##########################################

Data_type <- "Feather"                                              ##choose from "Feather", "Rdata" and "SQL" (data format, see next part)
Source.Table <- "etz3bsc1.[NT0001\\BD0587].[TTC_PD_1712_REC]"       ##REA data SQL table
PD_push.Table <- "[ETZ3BSC1].[NT0001\\BD0587].PDpush_ICAAP_1712_REC" ##PD push data SQL table
## case FSA:
#PD_push.Table <- "[ETZ3BSC1].[NT0001\\BC5216].PDpush_FSA_18_03" 
##case both ICAAP and FSE ina single run (not recomanded)!
# fsa <- "[ETZ3BSC1].[NT0001\\BC5216].PDpush_FSA_18_03"
Save_data <- "N"                                                    ##save data after loading (Y/N)
Save_data_format <- "Feather"                                       ##save data after loading format (Feather, Rdata)
Save_data_changed <- "N"                                            ##save data after loading (Y/N)
Save_data_changed_format <- "Feather"                               ##save data after loading format (Feather, Rdata)
FSA_run <- "N"                                                      ## FSA is run once a year. uses diff pd pushes 

Targets <- read_csv(paste(getwd(), "TTC_TARGETS_2017_12_11.csv", sep = "/"))
PD_pushes <- as_tibble(ExecuteSQL(paste ("select * from ", PD_push.Table, sep = "")))
##case both ICAAP and FSE ina single run (not recomanded)!
# fsa <- "[ETZ3BSC1].[NT0001\\BC5216].PDpush_FSA_18_03"
# fsa_table <- as_tibble(ExecuteSQL(paste ("select * from ", fsa, sep = "")))
#PD_pushes <- PD_pushes %>% left_join(fsa_table, by="ip_id")
colnames(PD_pushes)


########1. Loading REA dataset################################################################

#####################################
#   Choose:                         #
#       1. Rdata format             #   
#       2. Feather format           #
#       3. Load directly from SQL   #
#####################################
#*******************************************************************************************
if (Data_type == "Rdata"){
  load(file.choose())
} else if (Data_type == "Feather"){
  TTC_data <- read_feather(file.choose())
} else if (Data_type == "SQL"){
  TTC_data <- as_tibble(ExecuteSQL(paste ("select * from ", Source.Table, sep = "")))
} else {
  print("No data format chosen (parameter Data_type), please choose between Rdata, Feather and SQL")
}

if (Save_data == "Y"){
  
  if (Save_data_format == "Rdata"){
    save(TTC_data, file = "C:\\Users\\BC5216\\Desktop\\TTC PD stress\\Used version\\TTC_PD_07_12_LATEST.RData")
  } else if (Save_data_format == "Feather") {
    write_feather(TTC_data, "C:\\Users\\BC5216\\Desktop\\TTC PD stress\\2018_03\\Recallibration\\Saved data sets\\TTC_PD_18_03.feather")
  }
  
}

gc()

## Cleaning REA data table to save space
###########################################################################
TTC_data <- TTC_data[, !(colnames(TTC_data) %in% c("AR_ID","KUNDEPLEJ_REGNR","BFORR2","BFORR2_NAVN","BFORR3", "BFORR3_NAVN","guarantor_mk","BRANCHE",
                                                  
                                                   "produkt_grp","ejendomssektor"))]


TTC_data <- TTC_data[, !(colnames(TTC_data) %in% c("exposure_class_5","EXPOSURE_TYPE_KD","RWA_USED_MK","ac_kl_bemaerk_5","exposure_dkk" ,
                                                   "lgd_roac" , "risk_weight_pm","org3niv", "IP_DATA_GR",
                                                   "INDUSTRY_PORTFOLIO_2",  "default_rwa_mk" ,"lgd_min_pm", "FINANCIAL_SECTOR" ,"maturity_years",
                                                   "KC_OMS_EUR","ead_amount_dkk", 
                                                   "r_rwa" ,"b_rwa", "rwa_dkk","exposure_group_eur"))]

########2. Cleaning TTC target data###########################################################

##############################################################################
##  Clean the target data table and create additional table                 ##
##  with targets grouped by TTC segment (later used for alpha calibration)  ##
##############################################################################

Targets <- Targets %>%
  Target_allocation_cleaning_targets_pipeline()

Targets_2 <- Targets %>% filter(!grepl("_FI", SEGMENT))

Targets_grouped <- Targets_2 %>%
  Target_grouping()  %>%
  select(TTC_SEGMENT, SEGMENT_1, TTC_TARGET)

gc()

colnames(TTC_data)

gc()


TTC_data <- TTC_data %>% mutate(PD_PIT_PM_OLD_back = PD_PIT_PM_OLD)
quality_check <- TTC_data %>% select(IP_ID) 
colnames(quality_check)

colnames(TTC_data)

TTC_data_bckp <- TTC_data

##################################################################################
# define scenarios
#################################################################################
# use the same naming as in PDPush engine
#if you wish to run it all use: 
# scenario_icaap <- list("BC_Y1","BC_Y2", "BC_Y3","MR_Y1", "MR_Y2", "MR_Y3", "SR_Y1", "SR_Y2", "SR_Y3", "SR2_Y1","SR2_Y2","SR2_Y3","ER_Y1","ER_Y2", "ER_Y3")
# case fsa: scenario_icaap <- list("FSA_BC_Y1","FSA_BC_Y2","FSA_SS_Y1","FSA_SS_Y2")
#scenario_icaap <- list("SR2_Y3","BC_Y2","BC_Y3")
scenario_icaap <- list("Base_Y1","Base_Y2", "Base_Y3","UT_Y1", "UT_Y2", "UT_Y3", "DT_Y1", "DT_Y2", "DT_Y3")

gc()

start_time <- Sys.time()

for (scenario in scenario_icaap) {
  #PDPush = PDPush+"scenario"
  TTC_data <- TTC_data_bckp
  PDPush_name <- paste("PDPush_",scenario,sep="", collapse=NULL)
  TTC_PD_name <- paste("TTC_PD_",scenario,sep="", collapse=NULL)
  print(PDPush_name)
  colnames(PD_pushes)
  
  #TTC_data <- TTC_data %>% left_join(PD_pushes, by = c("IP_ID" = "ip_id"))
  colnames(TTC_data)
  gc()
  
  #quality_check <- TTC_data %>% select(IP_ID)

  merge_pd <- select(PD_pushes,ip_id,BRANCHE, PRODUKT_GRP, EJENDOMSSEKTOR, EXPOSURE_CLASS_5, ICAAP_SECTOR,.dots=PDPush_name)
  print(colnames(merge_pd))

  
  
  TTC_data <- TTC_data %>% left_join(merge_pd, by = c("IP_ID" = "ip_id"))
  
  #TTC_data <- TTC_data %>% mutate(PD_PIT_PM_OLD = PD_PIT_PM_OLD_back)
  
  
  TTC_data <- TTC_data %>% mutate(PD_PIT_PM_OLD = pmin(PD_PIT_PM_OLD_back * .dots, 0.999999))
  test_ttc_temp <- TTC_data$PD_PIT_PM_OLD
  #setNames(TTC_data, old=".dots", new=PDPush_name)
  colnames(TTC_data)[colnames(TTC_data)==".dots"] <- PDPush_name
  
  print (colnames(TTC_data))

  
  print("Checkpoint 1!")
  
  ########3. Data cleaning for alpha callibration###############################################
  
  
  TTC_data <- TTC_data %>%
    Property_NOP_pipeline() %>%                   ## Special treatment for Property clients 
    FIRB_AIRB_flag() %>%                          ## adds a flag that identifies knids with exposure both in AIRB and FIRB
    TTC_target_allocation_pipeline(Targets) %>%   ## allocates TTC targets for each entry in the REA dataset
    Data_split_pipeline() %>%                     ## splits the data into different chunks to be treated separetely as callibrating alphas,
    Floor_allocation_pipeline()                   ## allocate regulatory floors for rgeulatory PD callibration

  gc()
  print("Cleaning data done for alpha done!")
  
  #########4. First step of Alpha callibration##################################################
  
  Alphas_1 <- TTC_data %>% 
    filter(split_2 == 1) %>%
    Calibrate_alpha_pipeline(Targets_grouped, 1) %>% ## initiation of alpha callibration (adress to the function for more info)
    mutate(Alpha = as.numeric(Alpha))                ## transform alpha output into numeric format
  
  gc()
  print("first step of alpha done!") 
  
  #########################5. Allocation of callibrated Alphas##################################
  
  TTC_data_1 <- TTC_data %>% filter(split_2 == 1) %>% Alocate_alphas(Alphas_1, 1) %>% Regulatory_PD()
  gc()
  print("step 5 done!!")
  
  #########6. Second step of Alpha callibration#################################################
  
  Targets_grouped_new <- Targets %>% 
    filter(grepl("_FI", SEGMENT)) %>% 
    Target_grouping() %>% 
    left_join(TTC_data %>% filter(grepl("_FI", TTC_SEGMENT)) %>% group_by(TTC_SEGMENT) %>% summarise(distincts = n_distinct(knid)), by = c("TTC_SEGMENT" = "TTC_SEGMENT")) %>% 
    left_join(TTC_data_1 %>% filter(grepl("_FI", TTC_SEGMENT), split_2 == 1) %>% Distinct_knid_pipeline() %>% group_by(TTC_SEGMENT) %>% summarise(SUM_PD = sum(MOD_REGULATORY_PD)), by = c("TTC_SEGMENT" = "TTC_SEGMENT")) %>% 
    mutate(SUM_PD = coalesce(SUM_PD, 0),
           ADJUSTED_TTC_TARGET = TTC_TARGET - SUM_PD/distincts) %>%
    select(TTC_SEGMENT, distincts, ADJUSTED_TTC_TARGET)
  gc()
  
  Alphas_2 <- TTC_data %>% 
    filter(split_2 == 2) %>%
    Calibrate_alpha_pipeline(Targets_grouped_new, 2) %>%   ## initiation of alpha callibration (adress to the function for more info)
    mutate(Alpha = as.numeric(Alpha))                      ## transform alpha output into numeric format
  
  gc()
  ##############################################################################################
  print("step 6 done!!")
  
  #########################7. Allocation of callibrated Alphas##################################
  
  TTC_data_2 <- TTC_data %>% filter(split_2 == 2) %>% Alocate_alphas(Alphas_2, 2) %>% Regulatory_PD()
  gc()
  
  print("step 7 done!!")
  
  #########################8. Assign masterscale grades#########################################
  
  TTC_data_2 <- TTC_data_2 %>% mutate(Target = as.character(Target))
  TTC_data_2 <- TTC_data_2 %>% mutate(Cases = as.character(Cases))
  TTC_data <- TTC_data_1 %>% bind_rows(TTC_data_2)
  
  rm(TTC_data_1)
  rm(TTC_data_2)
  gc()
  print("step 8 done!!")
  
  #########################9. Assign masterscale grades#########################################
  
  TTC_data <- TTC_data %>% PD_masterscale()
  gc()
  print("step 9 done!!")
  
  #####################Some data needed for testing and timestamp vector########################
  
  
  TTC_data <- TTC_data %>% mutate(PD_diff = (MOD_REGULATORY_PD - pd_min_pm)/pd_min_pm)
  
  print ("PD_diff created")
  #Temporary commented
  #***********************
  #diff_var = paste("PD_diff_",scenario,sep="", collapse=NULL)
  #quality_check <- quality_check %>% mutate(diff_temp= TTC_data$PD_diff)
  #names(quality_check)[names(quality_check)=='diff_temp'] <- sprintf("%s",diff_var)
  #quality_check <- quality_check %>% mutate(MOD_REGULATORY_PD_temp = TTC_data$MOD_REGULATORY_PD)
  #quality_check <- quality_check %>% mutate(pd_min_pm_temp = TTC_data$pd_min_pm)
  #names(quality_check)[names(quality_check)=="MOD_REGULATORY_PD_temp"]<-sprintf("MOD_REGULATORY_PD_%s",scenario)
  #names(quality_check)[names(quality_check)=="pd_min_pm_temp"]<-sprintf("pd_min_pm_%s",scenario)
                                            
 
                                            
  
  #*************************************************
  #filename_output =paste("..\\Results\\sanity_check\\output_",scenario,".txt",sep="", collapese=NULL)
  #file.create(filename_output)
  #fileConn<-file(filename_output)
  #sink(fileConn,split=FALSE,append = FALSE)
  
  #cat("SEGMENT,result1,result2",file=fileConn,sep="\n",append=TRUE)
  #for(i in pull(distinct(select(TTC_data, TTC_SEGMENT)))){
   # cat(i,file=fileConn,sep="\n",append=TRUE)
    #cat(min(TTC_data %>% filter((TTC_SEGMENT == i)) %>% select(PD_diff)),file=fileConn,sep="\n",append=TRUE)
    #cat(max(TTC_data %>% filter((TTC_SEGMENT == i)) %>% select(PD_diff)),file=fileConn,sep="\n",append=TRUE)
    ##print(i)
    ##print(min(TTC_data %>% filter((TTC_SEGMENT == i)) %>% select(PD_diff)))
    ##print(max(TTC_data %>% filter((TTC_SEGMENT == i)) %>% select(PD_diff)))
  #}
  #close(fileConn)
  #print("writing output completed")
  #*************************************************
  
  
  test <- glimpse(TTC_data %>% filter((TTC_SEGMENT == "property_FI")) %>% arrange(desc(PD_diff)))
  test <- glimpse(TTC_data %>% filter((TTC_SEGMENT == "property_FI")) %>% arrange(PD_diff))
  
  
  #TTC_data %>% summarise((sum(RWA1) - sum(rwa_dkk))/sum(rwa_dkk))
  ##############################################################################################
  # SAVE THE AVERAGE TTC_PD PER SECTOR AND THE DIFFERENCE TO THE TARGET
  
  check_1 <- TTC_data %>% select(IP_ID, TTC_SEGMENT, MOD_REGULATORY_PD) %>% group_by(IP_ID) %>% 
    summarise(temp_var = max(MOD_REGULATORY_PD), TTC_SEGMENT=max(TTC_SEGMENT))
  
  check_1 <- check_1 %>% group_by(TTC_SEGMENT) %>% summarise(avg = mean(temp_var))
  
  check_2 <- check_1 %>% inner_join(Targets_grouped,by="TTC_SEGMENT")
  check_2 <- check_2 %>% mutate(diff_target = abs(avg-TTC_TARGET))
  
  if (exists("check_all") && is.data.frame(get("check_all")) ){
    check_2 <- check_2[,!colnames(check_2) %in% c("SEGMENT","ttc_target","SEGMENT_1") ]
    check_all <- check_all %>% mutate(diff_target = check_2$diff_target)
    check_all <- check_all %>% mutate(avg = check_2$avg)
  } else {
    check_2 <- check_2 %>% mutate(diff_target = abs(avg-TTC_TARGET))
    check_all <- check_2}
  n1 = paste("Seg_avg_",scenario,sep="", collapse=NULL)
  n2 = paste("diff_",scenario,sep="", collapse=NULL)
  names(check_all)[names(check_all)=="avg"]<-sprintf("%s",n1)
  names(check_all)[names(check_all)=="diff_target"]<-sprintf("%s",n2)
  ##############################################################################################
  # SAVE ALPHAS
  Alphas_1 <- Alphas_1 %>% mutate(Target = as.numeric(Target))
  Alphas_1 <- Alphas_1 %>% mutate(Cases = as.numeric(Cases))
  ALF1 <- Alphas_1 %>% bind_rows(Alphas_2)
  a1 = paste("Alpha_",scenario,sep="", collapse=NULL)
  
  if (exists("ALF") && is.data.frame(get("ALF")) ){
    ALF <- ALF %>% mutate(alf_temp = ALF1$Alpha)
    names(ALF)[names(ALF)=="alf_temp"]<-sprintf("%s",a1)
  } else {
    ALF <- ALF1
    ALF <- ALF[,!colnames(ALF) %in% c("Target","Cases") ]
    names(ALF)[names(ALF)=="Alpha"]<-sprintf("%s",a1)
  }
  ##############################################################################################
  exportas_huge <- TTC_data %>% select(IP_ID, MOD_REGULATORY_PD) %>% group_by(IP_ID) %>% summarise(ttc_temp = max(MOD_REGULATORY_PD))
  names(exportas_huge)[names(exportas_huge)=="ttc_temp"]<-sprintf("%s",TTC_PD_name)
  csv_name <- paste("..\\Results\\TTC_PD_",scenario,".feather",sep="", collapse=NULL)
  write_feather(exportas_huge, csv_name)
  colnames(exportas_huge)
  #csv_name <- paste("C:\\Users\\Bd2204\\Documents\\TTC_PD\\Results\\TTC_PD_",scenario,".csv",sep="", collapse=NULL)
  #csv_name <- paste("..\\Results\\TTC_PD_",scenario,".feather",sep="", collapse=NULL)
  #print(csv_name)
  #write_csv(exportas_huge, csv_name)# "C:\\Users\\BC5216\\Desktop\\TTC PD stress\\2018_03\\Results\\TTC_PD_ER_Y2.csv")
  rm(Targets_grouped_new)
  rm(exportas_huge)
  
  rm(ALF1)
  rm(Alphas_1)
  rm(Alphas_2)
  colnames(TTC_data)
  

  
  #*****************************************************************
  
  
  TTC_data <- TTC_data[, (colnames(TTC_data) %in% c("IP_ID"           ,   "knid",               "rwa_relevant_mk",    "ac_b_type_navn" ,   "PD_TTC_PM" ,
                                                   "AC_KL_MODEL_ID" ,    "rwa_method_kd"    ,      "PD_PIT_PM_OLD"    ,  "residensland"    ,   "pd_min_pm" ,    
                                                  "Country_Split" ,     "AC_KL_MODEL_ID_new", "AIRB_FIRB"    ,"PD_PIT_PM_OLD_back","split_2","split_1"    
                                                    ))]    

}


#paste("..\\Results\\sanity_check\\quality_check",scenario,".csv",sep="", collapse=NULL)
write_csv(check_all, "..\\Results\\sanity_check\\quality_check.csv")#
write_csv(ALF, "..\\Results\\sanity_check\\Alphas.csv")#
#write_csv(check_all, csv_name2)#

end_time <- Sys.time()
  
Time <- end_time - start_time
print(Time)
#########################################################################################################################
#
#MERGE RESULTS AND PERFORM SANITY CHECK ON PDs (Sanity using REA is done separatelly)
#########################################################################################################################
# set working directory
setwd("..\\Results\\")
getwd()# list all csv files from the current directory
# Remember you changed working directory. If you wish to move UP in the code and re-run things, make sure you go back to R_code directory: Session->Set working directory
list.files(pattern=".feather$") # use the pattern argument to define a common pattern  for import files with regex. Here: .csv

# create a list from these files
list.filenames<-list.files(pattern=".feather$")
ll =list.filenames

# create an empty list that will serve as a container to receive the incoming files
list.data<-list()

merged_ttcpd <- data.frame(read_feather(ll[1]))
merged_ttcpd <- merged_ttcpd$IP_ID


for (j in ll){
  print(j)
  temp_data <- read_feather(j)
  temp_data <- data.frame(temp_data[,2]) #[,!colnames(temp_data) %in% c("IP_ID")]
  print(colnames(temp_data))
  merged_ttcpd <- cbind(merged_ttcpd,temp_data)

}

colnames(merged_ttcpd)
write_csv(merged_ttcpd, "all_TTC_PD.csv")

# Plot obtained TTC_PDs for all the scenarios as a sanity check
# errors may occur due to the too small margins. Try enlarging the 
#Plot window in R studio or choose to plot fewer scenarios at the time
# set scenarios as x_cols <- c("TTC_PD_BC_Y1","TTC_PD_SR_T2") 

x_cols <- c(colnames(merged_ttcpd[,-1]))
x_cols <- c("TTC_PD_BC_Y1","TTC_PD_SR_T2")

attach(mtcars)
#par(mfrow=c(2,2))
#dev.off()
for (i in x_cols)
{
  print(i) 
  x <- as.numeric(unlist(merged_ttcpd[i])) #temp_data %>% mutate(temp_data = as.numeric(unlist(temp_data)))
  hx <- dnorm(x)


  plot(x, hx, type="l", lty=2, xlab=i,
       ylab="Density")
  

}

#rm(merged_ttcpd)
# REMOVE ALL THE SINGLE .feather FILES 
for (j in ll){
  fn <- j
  if (file.exists(fn)) file.remove(fn)
}
