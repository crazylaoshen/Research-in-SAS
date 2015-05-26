%let shen=D:\google drive\NC semester 3\AAEA\data;
libname shen "&shen";
/*This data statemnent reads in all text files that start with SOBSCC
  Since all of the .txt files are identical there is nothing special that needs to be done*/

/*I noticed in the data set that the county codes are state specific, that is there is a 001 in AL and there is a 001 in AK
  Thus I made a StateCountyID variable that picks off the state abbreviation and the county code to be later be used for sorting and aggregating by county*/
data SOBSCC;
  infile "&shen.\SOBSCC*.txt" dlm='|' firstobs=1 dsd missover;
  input CropYear  StateCode StateAbr :$2. CountyCode CountyName :$35. CropCode:$3.  CropName:$30.  InsurancePlanCode :$2. InsurancePlanName :$5. CoverageCategory :$5. 
        PolicySold PolicyEarn PolicyIndem NetAcrage Liability TotalPremium Subsidy Indemnity LossRatio;
  if upcase(CropName) in ('BARLEY', 'CORN', 'COTTON', 'GRAIN SORGHUM', 'OATS', 'RICE', 'SOYBEANS', 'SUGAR BEETS', 'WHEAT');
  keep CropYear StateCode CropName CropCode StateAbr StateCountyID PolicySold NetAcrage Liability TotalPremium Subsidy Indemnity;
  StateCountyID = catx('_', StateAbr,CountyCode); 
run;

/*This data statement reads in the csv file with the farm number data*/
/*NOTE: This data is only for every 5th year between 1981 and 2013; 
        For the other years I just linearly extrapolate over the missing years
        However this specific data set does not include the missing values and only has observations for every fifth year
        The 2012 census data have not been published yet, so we only have the data to year 2007.
        We will need a merge that creates the missing values and then use that data set to extrapolate over*/
/*NOTE: The crop names are not all the same as those in the SOBSCC files.  
        As such we rename certain crop variables in this data step to match the SOBSCC files*/

data farms;
  infile "&shen.\farm numbers.csv" dlm=',' firstobs=2 missover;
  input CropYear CropName :$30. FarmNumber:30.;
  if(CropName = "SORGHUM") then CropName = "GRAIN SORGHUM"; 
  if(CropName = "SUGARBEETS") then CropName = "SUGAR BEETS"; 
run;
/*This data step reads in the csv file with the production data that includes the Value variable needed for analysis*/
/*This data should be used to create a ValueShare variable that is the percentage of the value for each crop in a given year*/
/*NOTE: The crop names are not all the same as those in the SOBSCC files.  
        As such we rename certain crop variables in this data step to match the SOBSCC files*/
data production;
  infile "&shen.\production in dollars.csv" dlm=',' firstobs=2 dsd missover;
  input Program :$6. CropYear Period :$4. GeoLevel :$8. State :$8. CropName :$14. DataItem :$50. Domain :$30. Value :comma.;
  if(CropName = "SORGHUM") then CropName = "GRAIN SORGHUM"; 
  if(CropName = "SUGARBEETS") then CropName = "SUGAR BEETS";
  keep CropYear CropName Value;
run;
/**********************************************************************************************************/
/******************************WORKING ON THE SOBSCC DATASET***********************************************/
/**********************************************************************************************************/

/*Sorting the SOBSCC data set by the Year then by the Crop and finally by the State County ID*/
/*This will allow us to find the national and county averages as desired later in the program*/
proc sort data=SOBSCC;
	by CropYear CropName StateAbr StateCountyID;
run;
/*This means procedure creates a National data set that aggregats all of the state/county data to get a national total for each varaible by both year and crop*/
/*NOTE: Nrecord = _FREQ_ will be added to the data set as a variable that counts the number of records used in the sum*/
proc means data=SOBSCC noprint;
  var PolicySold /*NetAcrage*/ Liability TotalPremium Subsidy Indemnity;
  by CropYear CropName;
  output out=National(drop=_TYPE_ rename=_FREQ_ = Nrecord) Sum=;
run;
data State_Acrage;
  infile "D:\google drive\NC semester 3\AAEA\data\Acreage.csv" dlm=',' firstobs=2 missover;
  input CropYear State:$30. CropName :$30. StateAcrage;
  if(CropName = "SORGHUM") then CropName = "GRAIN SORGHUM"; 
  if(CropName = "SUGARBEETS") then CropName = "SUGAR BEETS"; 
run;
proc sort data=State_Acrage;
 by CropYear CropName State;
 run;
Proc means data=State_Acrage noprint;
var StateAcrage;
by CropYear CropName;
output out=National_Acrage(drop=_FREQ_ _TYPE_) Sum=NationalAcrage;
run;




/*This means procedure creates the county level sums for the variable NetAcrage so that we can compute the share variable */
/*proc means data=SOBSCC noprint;*/
/*  var NetAcrage;*/
/*  by CropYear CropName StateCountyID;*/
/*  output out=County_Acrage(drop=_FREQ_ _TYPE_) Sum=;*/
/*run;*/
/*This means procedure creates the state level sums for the variable NetAcrage so that we can compute a different definition of the share variable*/
/*proc means data=SOBSCC noprint;*/
/*  var NetAcrage;*/
/*  by CropYear CropName StateAbr;*/
/*  output out=State_Acrage(drop=_FREQ_ _TYPE_) Sum=;*/
/*run;*/

/*Merging the County_Acrage data with the National data so that we can compute the shares variable as specified*/
/*In this merge every variable in the National data set is repeated for every county in the data set*/
data shares;
  merge State_Acrage National_Acrage National ;
/*County_Acrage (rename = (NetAcrage=CountyAcrage)) */

  by CropYear CropName;
/*  ShareCounty = (CountyAcrage / NetAcrage)**2;*/
  ShareState = (StateAcrage / NationalAcrage)**2;
run;

/*Taking the sum of the share variable at the National level*/
proc means noprint;
  var /*ShareCounty*/ ShareState;
  by CropYear CropName;
  output out=ShareSums(drop=_FREQ_ _TYPE_) Sum=;
run;

/*Merging the share data at the national level with the rest of the national data*/
/*We also create other desired variables that need time series treatment to be used in their final form*/
data NationalwShares;
  merge National ShareSums;
  by CropYear CropName;
  Price = (TotalPremium - Subsidy) / Liability;
  LossRisk = Indemnity/ Liability;
run;
/**********************************************************************************************************/
/******************************WORKING ON THE FARM DATASET*************************************************/
/**********************************************************************************************************/

proc sort data=NationalwShares;
  by CropName CropYear;
run;

proc expand data=NationalwShares out=NationalwDif method=none;
      convert price=pricedif / transformout=(dif 1);
	  convert policysold=policydif / transformout=(dif 1);
	  convert price=pricelag / transformout=(lag 1);
	  convert policysold=policylag / transformout=(lag 1);
      convert LossRisk=lrisk_cumavr / transformout = (lag 1 cuave);
	  by CropName;
	  id CropYear;
run;

/*Resorting the Panel data set by crop name so that we can use the expand procedure*/
proc sort data=farms;
  by CropName CropYear;
run;


/*Merging with the NationalwDif dataset already created*/
data NationalwFarm;
  merge farms NationalwDif;
  by CropName CropYear;
run;

/*We are linearly extrapolating over the missing values for FarmNumber using the method=join in proc expand*/
/*NOTE: Since the csv file is missing the value for 2012 there are missing values in the FarmNumber variable from 2008-2012*/
proc expand data=NationalwFarm out=NationalwFarmEx extrapolate;
  convert FarmNumber / method=join;
  by CropName;
  id CropYear;
run;

/**********************************************************************************************************/
/******************************WORKING ON THE PRODUCTION DATASET*******************************************/
/**********************************************************************************************************/
/*Sorting by CropYear to take the mean over a given year*/
proc sort data=production;
  by CropYear CropName;
run;

proc means data=production noprint;
  var Value;
  by CropYear;
  output out=YearlyValue(drop=_TYPE_ _FREQ_ rename=(Value=YearlyValue)) Sum=;
run;

/*Creating a new dataset with the defined ValueShare variable*/
data ValueAll;
  merge production YearlyValue;
  by CropYear;
  ValueShare=Value/YearlyValue;
run; 
/**********************************************************************************************************/
/******************************MERGING IT ALL TOGETHER*****************************************************/
/**********************************************************************************************************/
/*First we sort both of the data sets by crop and then year  to make it look like a typical panel data set*/
proc sort data=ValueAll;
  by CropName CropYear ;
run;

proc sort data=NationalwFarmEx;
  by CropName CropYear;
run;
/*This data step merges together everything needed for the final panel dataset*/
/*It also calculates elasticity using the differences created earlier in the National datasets*/
/*It also creates a returns variable that can be used as a response in the panel regression*/
/*Since this is the last step we save the final data set to our permenant library*/
data Shen.FullPanel_jishen;
  merge NationalwFarmEx ValueAll;
  Elasticity = (Policydif/Policylag) / (Pricedif/Pricelag);
  Returns = (Indemnity - TotalPremium + Subsidy) / Liability;
  SubsidyRate=Subsidy/liability;
  if CropYear = 1981 then delete; 
  lnFarmNumber = log(FarmNumber);
  drop LnShareState;
/*  Dependent variable*/
  drop Pricedif Policydif Value YearlyValue;
run;

data year2007;
set shen.fullpanel_jishen;
if cropyear ne 2007 then delete;
run;


/*select year 2007 to examine the cross section effect to compare the panel data results.*/
/*Results shows that the concerntration plays a positive effect.*/
Title'Cross effect for year 2007';
proc reg data=year2007; 
  model SubsidyRate = ShareState lnFarmNumber ValueShare LossRisk Elasticity;
 
run;



/*Run proc panel with one time fixed effect, fixed effect and two fixed effect.
The result of first one is the most ideal, the result
of second one indicate the concentration has negative effect on returns which is 
not consistent with the theory*/
/*The variable elasticities shows insignificant in the regression.*/
/*Note:The Variable ShareState perform better than ShareCounty represent 
concerntration from the results*/
Title'Panel regression';
proc panel data=Shen.FullPanel_jishen;
   id  CropName CropYear ;

 model Returns = ShareState lnFarmNumber ValueShare LossRisk Elasticity/ fixonetime; 
 model Returns = ShareState lnFarmNumber ValueShare LossRisk Elasticity/ fixtwo;  


   model SubsidyRate = ShareState lnFarmNumber ValueShare LossRisk Elasticity/ fixonetime;
   model SubsidyRate = ShareState lnFarmNumber ValueShare LossRisk Elasticity/ fixtwo; 
run;
