rm(list=ls())
# Set working directory
setwd("C:\\Users\\saurabh.a.trivedi\\Desktop\\Protyping Code")
#set java path to access java library
Sys.setenv(JAVA_HOME='C:\\Program Files\\Java\\jre1.8.0_77')

#install this package to run erf() function
library(pracma)
# install this package to run nonlinear regression function nlsLM
library(minpack.lm)
#install this package as this is mandatory package for proper functioning of xlconnect
#library(rJava)
#install this package as this is mandatory package for variable content seprater using seprate function which is just like delimate in excel
library(tidyr)
#library to call rmse function
library(hydroGOF)

#initial parameter setting for curve fitting
strcount_filter=150
avgsales_flter=250
bucket_width=0.25
curve_split<-0.5
alpha_multiplier<-1.1

#Productivity parameters
sales_weight<-0.7
profit_weight<-0.2
units_weight<-0.1

#Select Store Group type a) productivity or b) climate
type<-"productivity"

#read store, product, space and VPE variables input data from CSV
big_master_data<-read.csv("Misses_Plus Brand_Curve Fitting Input_Sales D v Space.csv",header=TRUE,sep=",")
if (type=="climate") {
  store_climate<-read.csv("store_climate.csv",header=TRUE,sep=",")
}
#Read necessary CSV files for the calculation of bounds
AvgData<-big_master_data[,c(1,2,3)]
Parameters<-read.csv("Bound_Parameters.csv",header=TRUE,sep=",")

mydir <- getwd()
Future_Trgt_BA_Space<-AvgData
for(s in (1:nrow(Future_Trgt_BA_Space))){
Future_Trgt_BA_Space$Target.BA.Space[s]<-sum(Future_Trgt_BA_Space$Space[which(Future_Trgt_BA_Space$Store %in% Future_Trgt_BA_Space$Store[s])])
}
Future_Trgt_BA_Space<-Future_Trgt_BA_Space[!duplicated(Future_Trgt_BA_Space[c("Store", "Target.BA.Space")]),]
Future_Trgt_BA_Space<-Future_Trgt_BA_Space[,-c(2,3)]
  Brand_Entry<-AvgData
  Brand_Entry$Space.Hold.for.Brand.Entry<-0
  Brand_Entry<-Brand_Entry[,-c(2,3)]
  Brand_Entry<-Brand_Entry[!duplicated(Brand_Entry[c("Store", "Space.Hold.for.Brand.Entry")]),]

  Brand_Exit<-AvgData
  Brand_Exit$Exit<-0
  Brand_Exit<-Brand_Exit[,-3]
  Brand_Exit<-Brand_Exit[!duplicated(Brand_Exit[c("Store", "Exit")]),]
  Overwrite<-NULL

##########################################################################################################
#Bound Setting calculation
##################################################
#Name alignment for some of the variable
names(Brand_Entry)[2]<-"Space.Hold.for.Brand.Entry"
names(Future_Trgt_BA_Space)[2]<-"Target.BA.Space"

#merging all different files
masterdata<-merge(AvgData,Brand_Entry,by="Store",all.x=TRUE)
masterdata<-merge(masterdata,Brand_Exit,by=c("Store","Product"),all.x=TRUE)
masterdata$Exit<-ifelse(is.na(masterdata$Exit)==TRUE,0,masterdata$Exit)
masterdata<-merge(masterdata,Future_Trgt_BA_Space,by="Store",all.x=TRUE)
masterdata$Trgt_BA_Space_Less_Brnd_Entry<-as.numeric(as.character(masterdata$Target.BA.Space))-masterdata$Space.Hold.for.Brand.Entry

#Creating specific varables needed for bound setting
masterdata$Change_LB<-floor(masterdata$Space*(1-Parameters[1,4])/Parameters[1,1])*Parameters[1,1]
masterdata$Change_UB<-ceiling(masterdata$Space*(1+Parameters[1,4])/Parameters[1,1])*Parameters[1,1]
for(t in (1:nrow(masterdata))){
  masterdata$Space_PCT[t]<-masterdata$Space[t]/sum(masterdata$Space[which(masterdata$Store==masterdata$Store[t])])
}

for(k in (1:nrow(masterdata))){
  masterdata$minSpace[k]<-min(masterdata$Space[which(masterdata$Product %in% masterdata$Product[k])])
  masterdata$maxSpace[k]<-max(masterdata$Space[which(masterdata$Product %in% masterdata$Product[k])])
  masterdata$PCT_Blw_Min_Space[[k]]<-max(0,masterdata$minSpace[k])*(1-as.numeric(Parameters[2]))
  masterdata$PCT_Abv_Max_Space[[k]]<-masterdata$maxSpace[k]*(1+as.numeric(Parameters[2]))
  #masterdata$Glbl_Space_LB[k]<-max(2,floor(as.numeric(masterdata$PCT_Blw_Min_Space[[k]])/as.numeric(Parameters[1]))*as.numeric(Parameters[1]))
  #masterdata$Glbl_Space_UB[k]<-max(ceiling(masterdata$PCT_Abv_Max_Space[[k]]/as.numeric(Parameters[1]))*as.numeric(Parameters[1]),masterdata$PCT_Blw_Min_Space_Adj[k]+2)
  masterdata$Glbl_Space_LB[k]<-floor(masterdata$PCT_Blw_Min_Space[[k]]/as.numeric(Parameters[1]))*as.numeric(Parameters[1])
  masterdata$Glbl_Space_UB[k]<-ceiling(masterdata$PCT_Abv_Max_Space[[k]]/as.numeric(Parameters[1]))*as.numeric(Parameters[1])
  masterdata$minSpacePCT[k]<-min(masterdata$Space_PCT[which(masterdata$Product %in% masterdata$Product[k])])
  masterdata$maxSpacePCT[k]<-max(masterdata$Space_PCT[which(masterdata$Product %in% masterdata$Product[k])])
  masterdata$PCT_Space_LB_PCT[k]<-masterdata$minSpacePCT[k]*(1-as.numeric(Parameters[3]))
  masterdata$PCT_Space_UB_PCT[k]<-masterdata$maxSpacePCT[k]*(1+as.numeric(Parameters[3]))
}
masterdata<-masterdata[order(masterdata$Store),]
Products_Key_metrics<-masterdata[,c("Product","minSpace","maxSpace","PCT_Blw_Min_Space","PCT_Abv_Max_Space","Glbl_Space_LB","Glbl_Space_UB","minSpacePCT","maxSpacePCT","PCT_Space_LB_PCT","PCT_Space_UB_PCT")]
Products_Key_metrics<-unique(Products_Key_metrics)
Overwrite<-reshape(Products_Key_metrics,times=names(Products_Key_metrics)[2:11],v.names="Value",varying=list(names(Products_Key_metrics)[2:11]),timevar="Variable" ,direction = "long",new.row.names=NULL)[,1:3]
rownames(Overwrite)<-NULL
#write.csv(Overwrite,"Bound Setting Input Overwriting Bounds.csv",row.names=FALSE)
#create store group variable by Productivity
if(type=="productivity"){
for(j in 1:nrow(big_master_data)){
  big_master_data$store_group_dummy[j]<-(sum(sales_weight*big_master_data$Sales[which(big_master_data$Store == big_master_data$Store[j])]) + sum(profit_weight*big_master_data$Profit[which(big_master_data$Store == big_master_data$Store[j])]) + sum(units_weight*big_master_data$Units[which(big_master_data$Store == big_master_data$Store[j])]))/sum(big_master_data$Space[which(big_master_data$Store == big_master_data$Store[j])])
}
big_master_data_backup<-big_master_data[!duplicated(big_master_data$Store),]$store_group_dummy
for(j in 1:nrow(big_master_data)){
  big_master_data$Store_Group[j]<-ifelse(big_master_data$store_group_dummy[j]<quantile(big_master_data_backup,0.25),"Low",ifelse(big_master_data$store_group_dummy[j]>quantile(big_master_data_backup,0.75),"High","Medium"))
}
big_master_data<-big_master_data[,-which(names(big_master_data) %in% "store_group_dummy")]
} else {
  big_master_data<-merge(big_master_data,store_climate,by="Store",all.x=TRUE)
  names(big_master_data)[which(names(big_master_data) %in% "Climate")]<-"Store_Group"
}
big_master_data<-big_master_data[c("Store","Product","Store_Group","Space","Sales","Profit","Units")]
big_master_data$Product<-paste0(big_master_data$Product,"-",big_master_data$Store_Group)
big_master_data<-big_master_data[,-which(names(big_master_data) %in% "Store_Group")]

#loop to filter Store, Product, Space or specific VPE variables to run curve fitting
for(b in (4:ncol(big_master_data))){
  if("Profit" %in% names(big_master_data)[b]){
    target<-"Profit"
  } else if("Sales" %in% names(big_master_data)[b]){
    target<-"Sales"
  }else if("Units" %in% names(big_master_data)[b]){
    target<-"Units"
  }
  master_data<-big_master_data[,c(1:3,b)]
  
#defining non linear function for nlslm
  predfun <- function(Alpha,Beta,Shift,Space) {
    r <- Alpha*erf((Space-Shift)/(sqrt(2)*Beta))
    #if (debug) cat(alpha,Beta,mean(r),"\n")
    return(r)
  }
  #sum of total space allocated across products for a given store and Break Point variable creation 
  produclist<-unique(master_data$Product)
  for(j in 1:nrow(master_data)){
    master_data$BA_Space[j]<-sum(master_data$Space[which(master_data$Store == master_data$Store[j])])
  }
  
  for(j in 1:nrow(master_data)){
    master_data[[paste0("Unscaled_Break_Point_",target)]][j]<-quantile(master_data$Space[which(master_data$Product %in% master_data$Product[j])],0.01)
  }
  
#Variable to control position of the Curve plot output in an excel file    
l=1
  
# Loop for Data filter by product
  for (k in 1:nrow(data.frame(produclist))){
    
#Data filter with sales variable
    testdata<-big_master_data[which(big_master_data$Product %in% produclist[k]),] 
    
#Data filter using VPE
    data<-master_data[which(master_data$Product %in% produclist[k]),]
    
#Allowing products for curve fitting where store count is greater than 150 and avg sales is greater than $250 or NA
    if(nrow(testdata)>strcount_filter && ifelse(is.na(avgsales_flter)!=TRUE ,sum(testdata$Sales)/nrow(testdata)>avgsales_flter,TRUE)){
      #testing condition for a product where unique of space is less than 2 and if its true cretaing random numbers for successfull nlslm regression
      FORMULA<-as.formula(paste0(target,"~predfun(Alpha,Beta,Shift,Space)"))
      #Creating data table to store Correlation and Elasticity Coefficients for each product
      if(length(unique(data$Space))<=2){
        data[,paste0("Correlation_",target)]<-cor(big_master_data[,c(target,"Space")])[1,2]
        data[,paste0("Elasticity_",target)]<-data.frame((lm(log(big_master_data[,target])~log(big_master_data$Space),data=big_master_data))[1])[2,1]
      lm_model<-lm(log(big_master_data[,target][which(big_master_data$Space>quantile(big_master_data$Space, curve_split))])~log(big_master_data$Space[which(big_master_data$Space>quantile(big_master_data$Space, curve_split))]),data=big_master_data)
      } else {
        data[,paste0("Correlation_",target)]<-cor(data[,c(target,"Space")])[1,2]
        data[,paste0("Elasticity_",target)]<-data.frame((lm(log(data[,target])~log(data$Space),data=data))[1])[2,1]    
      if(length(unique(data$Space[which(data$Space>quantile(data$Space, curve_split))]))<=1){
      lm_model<-lm(log(data[,target])~log(data$Space),data=data)
      } else {
      lm_model<-lm(log(data[,target][which(data$Space>quantile(data$Space, curve_split))])~log(data$Space[which(data$Space>quantile(data$Space, curve_split))]),data=data)
      }
      }
      data[,paste0("Curve_Split_Elasticity_",target)]<-data.frame(lm_model[1])[2,1]
      if(unique(data[,paste0("Elasticity_",target)])>0.1){
      Space_backup <- as.vector(1:nrow(data))
      if(length(unique(data$Space))<=2){
        for(r in (1:nrow(data))){
          Space_backup[r]<-data$Space[r]
          data$Space[r]<- runif(1,ifelse(data$Space[r]-(bucket_width)/2<=0,0.001,data$Space[r]-(bucket_width)/2),data$Space[r]+(bucket_width)/2)
          
        }
      }
      #log-log model model with an assumption that elasticity of the top half or top 25% of the data points will affect where the curve should asymptote
      spacetosolvefor<-mean(data$Space[which(data$Space<quantile(data$Space, curve_split))])
      salestosolvefor<-mean(data[,target][which(data$Space<quantile(data$Space, curve_split))])
      #starting values for nlslm coefficients
      data[,paste0("Alpha_Seed_",target)]<-exp(data.frame(lm_model[1])[1,1]+ data.frame(lm_model[1])[2,1]* log(alpha_multiplier*max(data$Space)))
      if(salestosolvefor>unique(data[,paste0("Alpha_Seed_",target)])){
        salestosolvefor<-0.95*unique(data[,paste0("Alpha_Seed_",target)])
      }
      if(unique(data[,paste0("Alpha_Seed_",target)])<mean(data[,target])){
        data[,paste0("Alpha_Seed_",target)]=mean(data[,target])
      }
      data[,paste0("Shift_Seed_",target)]<-0
      data[,paste0("Beta_Seed_",target)]<--(spacetosolvefor-data[,paste0("Shift_Seed_",target)])/qnorm((1-salestosolvefor/data[,paste0("Alpha_Seed_",target)])/2)
      #Lower bounds for nlslm coefficients
      Alpha_LB<-0
      Beta_LB<-0
      Shift_LB<-0
      #Upper bounds for nlslm coefficients
      data[,paste0("Alpha_UB_",target)]<-quantile(data[,target], 0.95)
      data[,paste0("Beta_UB_",target)]<-Inf
      if(min(data$Space)<=1){
      data[,paste0("Shift_UB_",target)]<-0
      } else {
        data[,paste0("Shift_UB_",target)]<-min(data$Space)-1
      }
      #Executing Non Linear regression model
      
      model<-try(model<-nlsLM(FORMULA,data=data,algorithm="LM",start=list(Alpha=unique(data[,paste0("Alpha_Seed_",target)]),Beta=unique(data[,paste0("Beta_Seed_",target)]),Shift=unique(data[,paste0("Shift_Seed_",target)])),lower=c(Alpha_LB,Beta_LB,Shift_LB),upper=c(data.frame(data[,paste0("Alpha_UB_",target)])[1,1],unique(data[,paste0("Beta_UB_",target)]),unique(data[,paste0("Shift_UB_",target)]))), silent = TRUE)
      if(grepl("Error",model[1])==TRUE && data[,paste0("Shift_UB_",target)]!=0){
      model<-nlsLM(FORMULA,data=data,algorithm="LM",start=list(Alpha=unique(data[,paste0("Alpha_Seed_",target)]),Beta=unique(data[,paste0("Beta_Seed_",target)]),Shift=0.5*min(data$Space)),lower=c(Alpha_LB,Beta_LB,Shift_LB),upper=c(data.frame(data[,paste0("Alpha_UB_",target)])[1,1],unique(data[,paste0("Beta_UB_",target)]),unique(data[,paste0("Shift_UB_",target)])))
      }
      #Extracting model results 
      coef<-data.frame(summary(model)[10][1])
      coef[,c(1,2,3)]<-round(coef[,c(1,2,3)],4)
      
      #Root mean square error calculation
      data[,paste0("RMS_Error_",target)]<-rmse(data[,target],predict(model,data$Space))
      
      #quasi rsquared value, so long as your model is reasonably close to a linear model and is pretty big
      data[,paste0("Quasi_R_Squared_",target)]<-1-sum((data[,target]-predict(model,data$Space))^2)/(length(data[,target])*var(data[,target]))
      
      data[,paste0("AIC_",target)]<-data.frame(AIC(model))
      data[,paste0("BIC_",target)]<-data.frame(BIC(model))
      
      #Cleaning variable names
      names(coef)<-c("Estimate","Std_Error","t_Value","p_value")
      coef$Parameters<-rownames(coef)
      rownames(coef)<-NULL
      coef<-coef[,c(5,1,2,3,4)]
      #resetting space values back to original space for cases where unique of space is less than 2 
      if(length(unique(Space_backup))<=2){
        data$Space<-Space_backup
        Space_backup <- as.vector(1:nrow(data))
      }
      c=1
      } else {
        Estimate<-c(mean(data[,target]),min(data$Space)/(sqrt(2)*qnorm(mean(c(mean(data[,target]),(sum(data[,target])/sum(data$Space))*min(data$Space)))/mean(data[,target]))),0)
        coef<-data.frame(dummy=1,Estimate)
        rownames(coef)<-c("Alpha","Beta","Shift")
        #Root mean square error calculation
        data[,paste0("Alpha_Seed_",target)]<-NA
        data[,paste0("Shift_Seed_",target)]<-NA
        data[,paste0("Beta_Seed_",target)]<-NA
        data[,paste0("Alpha_UB_",target)]<-NA
        data[,paste0("Beta_UB_",target)]<-NA
        data[,paste0("Shift_UB_",target)]<-NA
        data[,paste0("RMS_Error_",target)]<-NA
        data[,paste0("Quasi_R_Squared_",target)]<-NA
        data[,paste0("AIC_",target)]<-NA
        data[,paste0("BIC_",target)]<-NA
      }
      
      
      #Generating key unscalled and scaled variables by store
      data[,paste0("Unscaled_Alpha_",target)]<-coef[1,2]
      data[,paste0("Unscaled_Beta_",target)]<-coef[2,2]
      data[,paste0("Unscaled_Shift_",target)]<-ifelse(coef[3,2]>0 && coef[3,2]<1,round(coef[3,2],0),coef[3,2])
      data[,paste0("Unscaled_Break_Point_",target)]<-ifelse(data[,paste0("Unscaled_Shift_",target)]==0,0,data[,paste0("Unscaled_Break_Point_",target)])
      data[,paste0("Productivity_",target)]<-data[,target]/data$Space
      data[,paste0(target,"_BP")]<-data[,paste0("Unscaled_Alpha_",target)]*erf((data[,paste0("Unscaled_Break_Point_",target)]-data[,paste0("Unscaled_Shift_",target)])/(sqrt(2)*data[,paste0("Unscaled_Beta_",target)]))
      data[,paste0("Productivity_BP",target)]<-data[,paste0(target,"_BP")]/data[,paste0("Unscaled_Break_Point_",target)]
      data[,paste0("Predicted_",target)]<-ifelse(data$Space<data[,paste0("Unscaled_Break_Point_",target)],data$Space*data[,paste0("Productivity_BP",target)],data[,paste0("Unscaled_Alpha_",target)]*erf((data$Space-data[,paste0("Unscaled_Shift_",target)])/(sqrt(2)*data[,paste0("Unscaled_Beta_",target)])))
      if (is.na(unique(data[,paste0("Quasi_R_Squared_",target)]))==TRUE){
        data[,paste0("Quasi_R_Squared_",target)]<-1-sum((data[,target]-data[,paste0("Predicted_",target)])^2)/(length(data[,target])*var(data[,target]))
        data[,paste0("RMS_Error_",target)]<-rmse(data[,target],data[,paste0("Predicted_",target)])
      }
      
      data[,paste0("Scaling_Type_",target)]<-ifelse(data[,target]>=data[,paste0("Predicted_",target)],ifelse(data[,target]>=data[,paste0(target,"_BP")],"A","C"),ifelse(data$Space>=data[,paste0("Unscaled_Break_Point_",target)],"B","D"))
      data[,paste0("Scaled_Alpha_",target)]<-data[,paste0("Unscaled_Alpha_",target)]+data[,target]-data[,paste0("Predicted_",target)]
      data[,paste0("Space2Solve4_",target)]<-ifelse(data[,paste0("Scaling_Type_",target)]=="A" | data[,paste0("Scaling_Type_",target)]=="B",data$Space,ifelse(data[,paste0("Scaling_Type_",target)]=="D",data[,paste0("Unscaled_Break_Point_",target)],data[,paste0(target,"_BP")]/data[,paste0("Productivity_",target)]))
      data[,paste0(target,"2Solve4")]<-ifelse(data[,paste0("Scaling_Type_",target)]=="A" | data[,paste0("Scaling_Type_",target)]=="B",data[,target],ifelse(data[,paste0("Scaling_Type_",target)]=="D",data[,paste0("Unscaled_Break_Point_",target)]*data[,paste0("Productivity_",target)],data[,paste0(target,"_BP")]))
      data[,paste0("Scaled_Shift_",target)]<-ifelse(data[,paste0("Scaling_Type_",target)]=="B" | data[,paste0("Scaling_Type_",target)]=="D",data[,paste0("Unscaled_Shift_",target)],ifelse(data[,paste0("Space2Solve4_",target)]+data[,paste0("Unscaled_Beta_",target)]*qnorm((1-(data[,paste0(target,"2Solve4")]/data[,paste0("Scaled_Alpha_",target)]))/2)>0,data[,paste0("Space2Solve4_",target)]+data[,paste0("Unscaled_Beta_",target)]*qnorm((1-(data[,paste0(target,"2Solve4")]/data[,paste0("Scaled_Alpha_",target)]))/2),0))
      data[,paste0("Scaled_Beta_",target)]<-(data[,paste0("Scaled_Shift_",target)]-data[,paste0("Space2Solve4_",target)])/(qnorm((1-(data[,paste0(target,"2Solve4")]/data[,paste0("Scaled_Alpha_",target)]))/2))
      data[,paste0("Scaled_BP_",target)]<-ifelse(!data[,paste0("Scaling_Type_",target)]=="A",data[,paste0("Unscaled_Break_Point_",target)],data[,paste0("Scaled_Shift_",target)]-data[,paste0("Scaled_Beta_",target)]*(qnorm((1-(data[,paste0(target,"_BP")]/data[,paste0("Scaled_Alpha_",target)]))/2)))
      data[,paste0(target,"Scaled_BP")]<- data[,paste0("Scaled_Alpha_",target)]*erf((data[,paste0("Scaled_BP_",target)]-data[,paste0("Scaled_Shift_",target)])/(sqrt(2)*data[,paste0("Scaled_Beta_",target)]))
      data[,paste0("Productivity_ScaledBP_",target)]<-data[,paste0(target,"Scaled_BP")]/data[,paste0("Scaled_BP_",target)]
      data[,paste0("Predicted_",target,"_Final")]<-ifelse(data$Space<data[,paste0("Scaled_BP_",target)],data$Space*data[,paste0("Productivity_ScaledBP_",target)],data[,paste0("Scaled_Alpha_",target)]*erf((data$Space-data[,paste0("Scaled_Shift_",target)])/(sqrt(2)*data[,paste0("Scaled_Beta_",target)])))
      
      #creating products summary with unscalled coeffcients and key model metrics
        datasummary<-unique(data[,c(2,17,16,18,19,7,8,9,10,12,11,13,14,15,20,22,21,6)])
        datasummary<-separate(data = datasummary, col = Product, into = c("Product", "Store_Group"), sep = "\\-")
        data<-data[,c(1,2,3,4,28,31,32,33)]
        data<-separate(data = data, col = Product, into = c("Product", "Store_Group"), sep = "\\-")
      if (l==1){
        Product_Summary<-datasummary
        final<-data
        l=2
      } else {
        Product_Summary<-rbind(Product_Summary,datasummary)
        final<-rbind(final,data)
        l=l+1
      }
    }
    
  }
  #Resetting product counter variable
  l=1
  if (b==4){
    master_Product_Summary<-Product_Summary
    final_master<-final
  } else {
    master_Product_Summary<-merge(master_Product_Summary,Product_Summary,by=c("Product","Store_Group"),all.x=TRUE)
    final_master<-merge(final_master,final,by=c("Store","Product","Store_Group","Space"),all.x=TRUE)
  }
}

##############################################################
#2nd Bound Setting piece
#####################################
AvgData<-final_master
AvgData$Product<-paste0(AvgData$Product,"-",AvgData$Store_Group)
AvgData<-AvgData[,c(1,2,4)]

mydir <- getwd()
#Checking for the availability of other required files
# a)Brand Entry file
if("Brand_Entry.csv" %in% dir(mydir)){
  w=1
} else {
  w=0  
}
# b)Brand Exit file
if(("Brand_Exit.csv") %in% dir(mydir)){
  x=1
} else {
  x=0 
}
# c)Future Target BA Space file
if("Future Target BA Space.csv" %in% dir(mydir)){
  y=1
} else {
  y=0
}
# d)Bound Setting Input Overwriting Bounds file
# if("Bound Setting Input Overwriting Bounds.csv" %in% dir(mydir)){
#   z=1
# } else{
#   z=0
# }

# Whether or not user wants to upload future BA space - if not, target BA space is the same as historical BA space 
if(y==0){
  Future_Trgt_BA_Space<-AvgData
  for(s in (1:nrow(Future_Trgt_BA_Space))){
    Future_Trgt_BA_Space$Target.BA.Space[s]<-sum(Future_Trgt_BA_Space$Space[which(Future_Trgt_BA_Space$Store %in% Future_Trgt_BA_Space$Store[s])])
  }
  Future_Trgt_BA_Space<-Future_Trgt_BA_Space[!duplicated(Future_Trgt_BA_Space[c("Store", "Target.BA.Space")]),]
  Future_Trgt_BA_Space<-Future_Trgt_BA_Space[,-c(2,3)]
} else {
  Future_Trgt_BA_Space<-read.csv("Future Target BA Space.csv",header=TRUE,sep=",",check.names=FALSE)
}
# Whether or not user has any brand entry information - if not, target BA space without brand entry is the same as target BA space
if(w==0){
  Brand_Entry<-AvgData
  Brand_Entry$Space.Hold.for.Brand.Entry<-0
  Brand_Entry<-Brand_Entry[,-c(2,3)]
  Brand_Entry<-Brand_Entry[!duplicated(Brand_Entry[c("Store", "Space.Hold.for.Brand.Entry")]),]
} else {
  Brand_Entry<-read.csv("Brand_Entry.csv",header=TRUE,sep=",",check.names=FALSE)
}
#Whether or not user has any brand exit information - if not, there will not be any product/stores with LB/UB = 0 due to brand exit
if(x==0){
  Brand_Exit<-AvgData
  Brand_Exit$Exit<-0
  Brand_Exit<-Brand_Exit[,-3]
  Brand_Exit<-Brand_Exit[!duplicated(Brand_Exit[c("Store", "Exit")]),]
} else {
  Brand_Exit<-read.csv("Brand_Exit.csv",header=TRUE,sep=",",check.names=FALSE)
}
# #Whether or not user has any manual bound overwriting information - if not, all bounds remain as calculated
# if(z==0){
#   Overwrite<-NULL
# } else {
#   Overwrite<-read.csv("Bound Setting Input Overwriting Bounds.csv",header=TRUE,sep=",")
# }
##################################################
#Name alignment for some of the variable
names(Brand_Entry)[2]<-"Space.Hold.for.Brand.Entry"
names(Future_Trgt_BA_Space)[2]<-"Target.BA.Space"
if(x==1){
  Brand_Exit$Exit<-"Exit"
}

#merging all different files
masterdata<-merge(AvgData,Brand_Entry,by="Store",all.x=TRUE)
masterdata<-merge(masterdata,Brand_Exit,by=c("Store","Product"),all.x=TRUE)
masterdata$Exit<-ifelse(is.na(masterdata$Exit)==TRUE,0,masterdata$Exit)
masterdata<-merge(masterdata,Future_Trgt_BA_Space,by="Store",all.x=TRUE)
masterdata$Trgt_BA_Space_Less_Brnd_Entry<-as.numeric(as.character(masterdata$Target.BA.Space))-masterdata$Space.Hold.for.Brand.Entry

#Creating specific varables needed for bound setting
masterdata$Change_LB<-floor(masterdata$Space*(1-Parameters[1,4])/Parameters[1,1])*Parameters[1,1]
masterdata$Change_UB<-ceiling(masterdata$Space*(1+Parameters[1,4])/Parameters[1,1])*Parameters[1,1]
for(t in (1:nrow(masterdata))){
  masterdata$Space_PCT[t]<-masterdata$Space[t]/sum(masterdata$Space[which(masterdata$Store==masterdata$Store[t])])
}

for(k in (1:nrow(masterdata))){
  masterdata$minSpace[k]<-min(masterdata$Space[which(masterdata$Product %in% masterdata$Product[k])])
  masterdata$maxSpace[k]<-max(masterdata$Space[which(masterdata$Product %in% masterdata$Product[k])])
  masterdata$PCT_Blw_Min_Space[[k]]<-max(0,masterdata$minSpace[k])*(1-as.numeric(Parameters[2]))
  masterdata$PCT_Abv_Max_Space[[k]]<-masterdata$maxSpace[k]*(1+as.numeric(Parameters[2]))
  #masterdata$Glbl_Space_LB[k]<-max(2,floor(as.numeric(masterdata$PCT_Blw_Min_Space[[k]])/as.numeric(Parameters[1]))*as.numeric(Parameters[1]))
  #masterdata$Glbl_Space_UB[k]<-max(ceiling(masterdata$PCT_Abv_Max_Space[[k]]/as.numeric(Parameters[1]))*as.numeric(Parameters[1]),masterdata$PCT_Blw_Min_Space_Adj[k]+2)
  masterdata$Glbl_Space_LB[k]<-floor(masterdata$PCT_Blw_Min_Space[[k]]/as.numeric(Parameters[1]))*as.numeric(Parameters[1])
  masterdata$Glbl_Space_UB[k]<-ceiling(masterdata$PCT_Abv_Max_Space[[k]]/as.numeric(Parameters[1]))*as.numeric(Parameters[1])
  masterdata$minSpacePCT[k]<-min(masterdata$Space_PCT[which(masterdata$Product %in% masterdata$Product[k])])
  masterdata$maxSpacePCT[k]<-max(masterdata$Space_PCT[which(masterdata$Product %in% masterdata$Product[k])])
  masterdata$PCT_Space_LB_PCT[k]<-masterdata$minSpacePCT[k]*(1-as.numeric(Parameters[3]))
  masterdata$PCT_Space_UB_PCT[k]<-masterdata$maxSpacePCT[k]*(1+as.numeric(Parameters[3]))
}
masterdata$Target.BA.Space<-as.character(masterdata$Target.BA.Space)
masterdata$Target.BA.Space[is.na(masterdata$Target.BA.Space)]<-0
masterdata$Target.BA.Space<-as.numeric(masterdata$Target.BA.Space)

########################################################
#Doing bound overwriting treatment on prepared data
if(is.null(Overwrite)==FALSE) {
  Overwrite<-reshape(Overwrite, idvar = "Product", timevar = "Variable", direction = "wide")
  names(Overwrite)<-c(gsub("Value.","",names(Overwrite)))
  colsize<-ncol(masterdata)
  masterdata<-merge(masterdata,Overwrite,by="Product",all.x=TRUE)
  for(m in 1:(ncol(masterdata)-colsize)){
    names<- gsub(".y","",names(masterdata)[(colsize+1):ncol(masterdata)])[m]
    masterdata[,which(names(masterdata) %in% paste0(names,".x"))] <- ifelse(is.na(masterdata[,which(names(masterdata) %in% paste0(names,".y"))]), masterdata[,which(names(masterdata) %in% paste0(names,".x"))],masterdata[,which(names(masterdata) %in% paste0(names,".y"))])
    
  }
  names(masterdata)<-gsub("\\.x","",names(masterdata))  
  masterdata<-masterdata[,-c((colsize+1):ncol(masterdata))]
}

#########################################################
masterdata$PCT_Space_LB<-floor(masterdata$Target.BA.Space*masterdata$PCT_Space_LB_PCT/as.numeric(Parameters[1]))*as.numeric(Parameters[1])
masterdata$PCT_Space_UB<-ceiling(masterdata$Target.BA.Space*masterdata$PCT_Space_UB_PCT/as.numeric(Parameters[1]))*as.numeric(Parameters[1])

for(l in (1:nrow(masterdata))){
  masterdata$Unconstrained_LB[l]<-max(masterdata$Change_LB[l],masterdata$Glbl_Space_LB[l],masterdata$PCT_Space_LB[l])
  masterdata$Unconstrained_UB[l]<-min(masterdata$Change_UB[l],masterdata$Glbl_Space_UB[l],masterdata$PCT_Space_UB[l])
  masterdata$Constrained_LB[l]<-ifelse(masterdata$Exit[l]=="Exit",0,max(masterdata$Change_LB[l],masterdata$Glbl_Space_LB[l],masterdata$PCT_Space_LB[l]))
  masterdata$Constrained_UB[l]<-ifelse(masterdata$Exit[l]=="Exit",0,min(masterdata$Change_UB[l],masterdata$Glbl_Space_UB[l],masterdata$PCT_Space_UB[l]))
}


masterdata<-masterdata[order(masterdata$Store),] 
Products_Key_metrics<-masterdata[,c("Product","minSpace","maxSpace","PCT_Blw_Min_Space","PCT_Abv_Max_Space","Glbl_Space_LB","Glbl_Space_UB","minSpacePCT","maxSpacePCT","PCT_Space_LB_PCT","PCT_Space_UB_PCT")]
Products_Key_metrics<-unique(Products_Key_metrics)

masterdata<-separate(data = masterdata, col = Product, into = c("Product", "Store_Group"), sep = "\\-")
final_master<-merge(final_master,masterdata,by=c("Store","Product","Store_Group","Space"),all.x=TRUE)
final_master<-final_master[,-c(20:40)]
#Final Output
write.csv(master_Product_Summary, "Analytics_Reference_Data.csv",row.names=FALSE)
write.csv(final_master, "Output_Data.csv",row.names=FALSE)
