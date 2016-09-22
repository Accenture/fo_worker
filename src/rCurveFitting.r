  #Clear historical data from R server
  rm(list=ls())
  
  # Set working directory
  #setwd("C:\\Users\\alison.stern\\Documents\\Kohls\\FO Enhancements\\R Code\\3 R Functions_08.24.2016\\Curve Fitting")
  
  #install this package to run erf() function
  library(pracma)
  #Data manipulation task performed using tidyr package 
  library(tidyr)
  #Library to call optimization package that runs the curve fitting
  library(nloptr)
  
  #initial parameter setting for curve fitting
  Increment_Size=0.5
  
  #Productivity parameters
  sales_weight<-0.7
  profit_weight<-0.2
  units_weight<-0.1
  
  #Bound parameters
  PCT_Space_Change_Limit<-0.5
  
  #Select Optimization type Tiered or Drill_Down
  type<-"Tiered"
  
  #big_master_data$Trgt_BA_Space_Less_Brnd_Entry<-as.numeric(as.character(big_master_data$Future_Space))-big_master_data$Entry_Space
  big_master_data<-read.csv("output.csv",header=TRUE,sep=",")
  bound_input<-read.csv("Bound_Input1.csv",header=TRUE,sep=",")
  
  #Curve Fitting and Bound Setting Function design
curvefitting_boundsetting<-function(big_master_data,bound_input,Increment_Size,sales_weight,profit_weight,units_weight,PCT_Space_Change_Limit,type){
    strcount_filter=100
    avgsales_flter=200
    curve_split<-0.75  
    
  #bound percent variable creation
    names(bound_input)[1]<-"Product"
    big_master_data<-big_master_data[,which(names(big_master_data) %in% c("Store","Climate","VSG","Product","Space","Sales","Units","Profit","Exit_Flag","Future_Space","Entry_Space"))]
    big_master_data$PCT_Space_Change_Limit<-PCT_Space_Change_Limit
    big_master_data<-merge(big_master_data,bound_input,by="Product",all.x=TRUE)
    
  #create "Climate_Group" variable
    big_master_data$Climate_Group<-big_master_data$Climate
    big_master_data$Climate_Group<-as.character(big_master_data$Climate_Group)
    if(type=="Drill_Down"){
    for(j in 1:nrow(big_master_data)){
      big_master_data$Climate_Group[j]<-ifelse((big_master_data$Climate_Group[j]=="HOT" || big_master_data$Climate_Group[j]=="SUPER HOT"),"HOT & SH",big_master_data$Climate_Group[j])
    }
    } else {
    big_master_data$Climate_Group<-"NA"
    }
    
  #Filter out product/climate group combinations
  big_master_data$prod_climate<-paste0(big_master_data$Product,"-",big_master_data$Climate_Group)
  productlist<-unique(big_master_data$prod_climate)
  p=1
  
  #Filter store/product where sales<20$ or Space<0.1
  filtered_final<-NULL
  if(length(which(big_master_data$Sales<20))!=0){
    if(p==1){
      filtered_data<-big_master_data[which(big_master_data$Sales<20),]
      filtered_final<-filtered_data
      p=p+1
    } else {
      filtered_data<-big_master_data[which(big_master_data$Sales<20),]
      filtered_final<-rbind(filtered_final,filtered_data)
    }
    big_master_data<-big_master_data[-which(big_master_data$Sales<20),]
  } 
  if (length(which(big_master_data$Space<0.1))!=0){
    if(p==1){
      filtered_data<-big_master_data[which(big_master_data$Space<0.1),]
      filtered_final<-filtered_data
      p=p+1
    } else {
      filtered_data<-big_master_data[which(big_master_data$Space<0.1),]
      filtered_final<-rbind(filtered_final,filtered_data)
    }
    big_master_data<-big_master_data[-which(big_master_data$Space<0.1),]
  }
  #Filter store-product combination where there is too little volume/store count to build a curve
  for (i in (1:length(productlist))){
    testdata<-big_master_data[which(big_master_data$prod_climate %in% productlist[i]),]
    if(nrow(testdata)>strcount_filter && ifelse(is.na(avgsales_flter)!=TRUE ,sum(testdata$Sales)/nrow(testdata)>avgsales_flter,TRUE)){
      big_master_data<-big_master_data
    } else {
      #recording filtered data for later use 
      if(p==1){
        filtered_data<-big_master_data[which(big_master_data$prod_climate %in% productlist[i]),]
        filtered_final<-filtered_data
        p=p+1
      } else {
        filtered_data<-big_master_data[which(big_master_data$prod_climate %in% productlist[i]),]
        filtered_final<-rbind(filtered_final,filtered_data)
      }
      big_master_data<-big_master_data[-which(big_master_data$prod_climate %in% productlist[i]),]
    }
    
  }
  big_master_data<-big_master_data[,-which(names(big_master_data) %in% "prod_climate")]
  
  
  #Productivity Group preparation based on different rules
  for(j in 1:nrow(big_master_data)){
    big_master_data$productivity_group_dummy[j]<-(sales_weight*big_master_data$Sales[j] + profit_weight*big_master_data$Profit[j] + units_weight*big_master_data$Units[j])/big_master_data$Space[j]
  }
  big_master_data$str_climate<-paste0(big_master_data$Store,"-",big_master_data$Climate_Group)
  
  for(q in (1:nrow(big_master_data))){
    big_master_data_backup<-big_master_data[which(big_master_data$Climate_Group %in% big_master_data$Climate_Group[q]),]
    big_master_data_backup<-big_master_data_backup[which(big_master_data_backup$Product %in% big_master_data$Product[q]),]
    if(nrow(big_master_data_backup)>=600){
      big_master_data$Productivity_Group[q]<-ifelse(big_master_data$productivity_group_dummy[q]<quantile(big_master_data_backup$productivity_group_dummy,0.25),"Low",ifelse(big_master_data$productivity_group_dummy[q]>quantile(big_master_data_backup$productivity_group_dummy,0.75),"High","Medium"))
    } else if (nrow(big_master_data_backup)>=400){
      big_master_data$Productivity_Group[q]<-ifelse(big_master_data$productivity_group_dummy[q]<quantile(big_master_data_backup$productivity_group_dummy,0.5),"Low","High")  
    } else {
      big_master_data$Productivity_Group[q]<-"NA"
    }
  }
  big_master_data$Store_Group<-paste0(big_master_data$Climate_Group,"-",big_master_data$Productivity_Group)
  big_master_data<-big_master_data[,-which(names(big_master_data) %in% c("productivity_group_dummy","str_climate"))]
  big_master_data<-big_master_data[c("Store","Product","Store_Group","Space","Sales","Profit","Units","PCT_Space_Change_Limit","Space.Lower.Limit","Space.Upper.Limit","PCT_Space_Lower_Limit","PCT_Space_Upper_Limit","Exit_Flag","Future_Space","Entry_Space","Climate_Group","Productivity_Group","Climate","VSG")]
  big_master_data$Product<-paste0(big_master_data$Product,"|",big_master_data$Store_Group)
  #b=6
  #loop to filter Store, Product, Space or specific VPE variables to run curve fitting
  for(b in (5:7)){
    if("Profit" %in% names(big_master_data)[b]){
      target<-"Profit"
    } else if("Sales" %in% names(big_master_data)[b]){
      target<-"Sales"
    }else if("Units" %in% names(big_master_data)[b]){
      target<-"Units"
    }
    master_data<-big_master_data[,c(1:4,b,8:19)]
    #master_data<-big_master_data[,c(which(names(big_master_data) %in% c("Store", "Product")),b,8:19)]
    #sum of total space allocated across products for a given store and Break Point variable creation 
    produclist<-unique(master_data$Product)
    for(j in 1:nrow(master_data)){
      master_data$BA_Space[j]<-sum(master_data$Space[which(master_data$Store == master_data$Store[j])])
    }
    
    for(j in 1:nrow(master_data)){
      master_data[[paste0("Unscaled_Break_Point_",target)]][j]<-quantile(master_data$Space[which(master_data$Product %in% master_data$Product[j])],0.01)
    }
    
    #Variable to control position of the Curve plot output in an excel file    
    # Loop for Data filter by product
    l=1
    #k=17
    #loop to run Curve fitting by product and store
    for (k in 1:nrow(data.frame(produclist))){
      data<-master_data[which(master_data$Product %in% produclist[k]),]
      
      #Space_backup <- as.vector(1:nrow(data))
      if(length(unique(data$Space))<=1){
        data[,paste0("Correlation_",target)]<-NA
        Estimate<-c(mean(data[,target]),-((unique(data$Space)/2)-0)/qnorm((1-(mean(data[,target])/2)/mean(data[,target]))/2),0)
        coef<-data.frame(Estimate)
        coef<-data.frame(t(data.frame(coef)))
        colnames(coef)<-c("Alpha","Beta","Shift")
        data[,paste0("Alpha_Seed_",target)]<-NA
        data[,paste0("Shift_Seed_",target)]<-NA
        data[,paste0("Beta_Seed_",target)]<-NA
        data[,paste0("Alpha_UB_",target)]<-NA
        data[,paste0("Beta_UB_",target)]<-NA
        data[,paste0("Shift_UB_",target)]<-NA
        
      } else {
      
      #starting values for curve fitting coefficients
      data[,paste0("Correlation_",target)]<-cor(data[,c(target,"Space")])[1,2]  
      data[,paste0("Alpha_Seed_",target)]<-mean(data[,target][which(data$Space>=quantile(data$Space, curve_split))])
      if(unique(data[,paste0("Alpha_Seed_",target)])<0){
        data[,paste0("Alpha_Seed_",target)]<-1
      }
      spacetosolvefor<-mean(data$Space[which(data$Space<=quantile(data$Space, 1-curve_split))])
      salestosolvefor<-min(unique(data[,paste0("Alpha_Seed_",target)]), mean(data[,target][which(data$Space<=quantile(data$Space, 1-curve_split))]))
      data[,paste0("Shift_Seed_",target)]<-0
      
      data[,paste0("Beta_Seed_",target)]<--(spacetosolvefor-unique(data[,paste0("Shift_Seed_",target)]))/qnorm((1-salestosolvefor/unique(data[,paste0("Alpha_Seed_",target)]))/2)
      if(unique(data[,paste0("Beta_Seed_",target)])<0 || is.na(unique(data[,paste0("Beta_Seed_",target)]))==TRUE){
        data[,paste0("Beta_Seed_",target)]<-0
      }
      #Lower bounds for curve fitting coefficients
      Alpha_LB<-0
      Beta_LB<-0
      Shift_LB<-0
      #Upper bounds for curve fitting coefficients
      data[,paste0("Alpha_UB_",target)]<-quantile(data[,target], 0.95)
      data[,paste0("Beta_UB_",target)]<-Inf
      if(min(data$Space)<=1){
        data[,paste0("Shift_UB_",target)]<-0
      } else {
        data[,paste0("Shift_UB_",target)]<-min(data$Space)-1
      }
      
      #defining Sales-Space functions for optimization
      Space<-data$Space
      targetset<-data[,target]
      predfun <- function(par) {
        Alpha<-par[1]
        Beta<-par[2]
        Shift<-par[3]
        rhat <- Alpha*erf((Space-Shift)/(sqrt(2)*Beta))
        r<-sum((targetset - rhat)^2)
        #if (debug) cat(alpha,Beta,mean(r),"\n")
        return(r)
      }
      
      #Gradient setting for objective function
      gr <- function(par){
        Alpha<-par[1]
        Beta<-par[2]
        Shift<-par[3]
        c(erf(500*(mean(Space)-Shift)/(707*Beta)),
          (-(1000*Alpha*(mean(Space)-Shift)*exp(-(250000*(mean(Space)-Shift)^2)/(499849*Beta^2)))/(707*sqrt(22/7)*Beta^2)),
          (-(1000*Alpha*exp(-(250000*(mean(Space)-Shift)^2)/(499849*Beta^2)))/(707*sqrt(22/7)*Beta)))
      }
      #Storing the starting values in x0
      x0<-c(unique(data[,paste0("Alpha_Seed_",target)]),unique(data[,paste0("Beta_Seed_",target)]),unique(data[,paste0("Shift_Seed_",target)]))
      #Calling optimization function "auglag"
      model<-auglag(x0, predfun,gr=NULL, 
             lower=c(Alpha_LB,Beta_LB,Shift_LB),
             upper=c(data.frame(data[,paste0("Alpha_UB_",target)])[1,1],Inf,unique(data[,paste0("Shift_UB_",target)])),
             localsolver = c("MMA"))
      
      #Extracting model results 
      coef<-t(data.frame(model$par))
      }
      coef[,c(1,2,3)]<-round(coef[,c(1,2,3)],4)
      if(coef[1,2]==0){
        Estimate<-c(mean(data[,target]),min(data$Space)/(sqrt(2)*qnorm(mean(c(mean(data[,target]),(sum(data[,target])/sum(data$Space))*min(data$Space)))/mean(data[,target]))),0)
        coef<-data.frame(dummy=1,Estimate)
        rownames(coef)<-c("Alpha","Beta","Shift")
        coef<-coef[,-1]
        #coef<-t(data.frame(coef))
      }
      Prediction<-coef[1]*erf((data$Space-coef[3])/(sqrt(2)*coef[2]))
      
      #quasi rsquared value is calculated assuming nonlinear function to be reasonably close to a linear model
      data[,paste0("Quasi_R_Squared_",target)]<-1-sum((data[,target]-Prediction)^2)/(length(data[,target])*var(data[,target]))
      
      #Mean Absolute Percentage Error(MAPE) calculation
      data[,paste0("MAPE_",target)]<-mean(abs(Prediction-(data[,target]))/(data[,target]))
      
      #Generating unscalled and scaled variables by store
      data[,paste0("Unscaled_Alpha_",target)]<-coef[1]
      data[,paste0("Unscaled_Beta_",target)]<-coef[2]
      data[,paste0("Unscaled_Shift_",target)]<-coef[3]
      #data[,paste0("Unscaled_Shift_",target)]<-ifelse(coef[3]>0 && coef[3]<1,round(coef[3],0),coef[3])
      
      data[,paste0("Unscaled_Break_Point_",target)]<-ifelse(data[,paste0("Unscaled_Shift_",target)]==0,0,data[,paste0("Unscaled_Break_Point_",target)])
      data[,paste0("Productivity_",target)]<-data[,target]/data$Space
      data[,paste0(target,"_BP")]<-data[,paste0("Unscaled_Alpha_",target)]*erf((data[,paste0("Unscaled_Break_Point_",target)]-data[,paste0("Unscaled_Shift_",target)])/(sqrt(2)*data[,paste0("Unscaled_Beta_",target)]))
      data[,paste0("Productivity_BP",target)]<-data[,paste0(target,"_BP")]/data[,paste0("Unscaled_Break_Point_",target)]
      data[,paste0("Predicted_",target)]<-ifelse(data$Space<data[,paste0("Unscaled_Break_Point_",target)],data$Space*data[,paste0("Productivity_BP",target)],data[,paste0("Unscaled_Alpha_",target)]*erf((data$Space-data[,paste0("Unscaled_Shift_",target)])/(sqrt(2)*data[,paste0("Unscaled_Beta_",target)])))
      data[,paste0("Scaling_Type_",target)]<-ifelse(data[,target]>=data[,paste0("Predicted_",target)],ifelse(data[,target]>=data[,paste0(target,"_BP")],"A","C"),ifelse(data$Space>=data[,paste0("Unscaled_Break_Point_",target)],"B","D"))
      
      data[,paste0("Space2Solve4_",target)]<-ifelse(data[,paste0("Scaling_Type_",target)]=="A" | data[,paste0("Scaling_Type_",target)]=="B",data$Space,ifelse(data[,paste0("Scaling_Type_",target)]=="D",data[,paste0("Unscaled_Break_Point_",target)],data[,paste0(target,"_BP")]/data[,paste0("Productivity_",target)]))
      data[,paste0(target,"2Solve4")]<-ifelse(data[,paste0("Scaling_Type_",target)]=="A" | data[,paste0("Scaling_Type_",target)]=="B",data[,target],ifelse(data[,paste0("Scaling_Type_",target)]=="D",data[,paste0("Unscaled_Break_Point_",target)]*data[,paste0("Productivity_",target)],data[,paste0(target,"_BP")]))
      data[,paste0("Scaled_Alpha_",target)]<-0
      data[,paste0("Scaled_Shift_",target)]<-0
      data[,paste0("Scaled_Beta_",target)]<-0
      data[,paste0("Scaled_BP_",target)]<-0
      for(m in 1:nrow(data)){
        critical_space<-data[,paste0("Unscaled_Shift_",target)][m]+sqrt(2)*data[,paste0("Unscaled_Beta_",target)][m]*sqrt(log(sqrt(7/11)*(data[,paste0("Unscaled_Alpha_",target)][m]/data[,paste0("Unscaled_Beta_",target)][m])))
      data[,paste0("Scaled_Alpha_",target)][m]<-ifelse(data$Space[m]>critical_space,coef[1],data[,paste0("Unscaled_Alpha_",target)][m]+data[,target][m]-data[,paste0("Predicted_",target)][m])
      data[,paste0("Scaled_Shift_",target)][m]<-ifelse(data$Space[m]>critical_space,coef[3],ifelse(data[,paste0("Scaling_Type_",target)][m]=="B" | data[,paste0("Scaling_Type_",target)][m]=="D",data[,paste0("Unscaled_Shift_",target)][m],ifelse(data[,paste0("Space2Solve4_",target)][m]+data[,paste0("Unscaled_Beta_",target)][m]*qnorm((1-(data[,paste0(target,"2Solve4")][m]/data[,paste0("Scaled_Alpha_",target)][m]))/2)>0,data[,paste0("Space2Solve4_",target)][m]+data[,paste0("Unscaled_Beta_",target)][m]*qnorm((1-(data[,paste0(target,"2Solve4")][m]/data[,paste0("Scaled_Alpha_",target)][m]))/2),0)))
      data[,paste0("Scaled_Beta_",target)][m]<-ifelse(data$Space[m]>critical_space,coef[2],(data[,paste0("Scaled_Shift_",target)][m]-data[,paste0("Space2Solve4_",target)][m])/(qnorm((1-(data[,paste0(target,"2Solve4")][m]/data[,paste0("Scaled_Alpha_",target)][m]))/2)))
      data[,paste0("Scaled_BP_",target)][m]<-ifelse(data$Space[m]>critical_space,data[,paste0("Unscaled_Break_Point_",target)][m],ifelse(!data[,paste0("Scaling_Type_",target)][m]=="A",data[,paste0("Unscaled_Break_Point_",target)][m],data[,paste0("Scaled_Shift_",target)][m]-data[,paste0("Scaled_Beta_",target)][m]*(qnorm((1-(data[,paste0(target,"_BP")][m]/data[,paste0("Scaled_Alpha_",target)][m]))/2))))
      }
      data[,paste0(target,"Scaled_BP")]<- data[,paste0("Scaled_Alpha_",target)]*erf((data[,paste0("Scaled_BP_",target)]-data[,paste0("Scaled_Shift_",target)])/(sqrt(2)*data[,paste0("Scaled_Beta_",target)]))
      data[,paste0("Productivity_ScaledBP_",target)]<-data[,paste0(target,"Scaled_BP")]/data[,paste0("Scaled_BP_",target)]
      data[,paste0("Predicted_",target,"_Final")]<-ifelse(data$Space<data[,paste0("Scaled_BP_",target)],data$Space*data[,paste0("Productivity_ScaledBP_",target)],data[,paste0("Scaled_Alpha_",target)]*erf((data$Space-data[,paste0("Scaled_Shift_",target)])/(sqrt(2)*data[,paste0("Scaled_Beta_",target)])))
      data<-separate(data = data, col = Product, into = "Product", sep = "\\|")
      
      #creating products summary with unscalled coeffcients and important model metrics
      datasummary<-unique(data[c("Product","Store_Group",paste0("Quasi_R_Squared_",target),paste0("MAPE_",target),paste0("Correlation_",target),paste0("Alpha_Seed_",target),paste0("Beta_Seed_",target),paste0("Shift_Seed_",target),paste0("Alpha_UB_",target),paste0("Beta_UB_",target),paste0("Shift_UB_",target),paste0("Unscaled_Alpha_",target),paste0("Unscaled_Beta_",target),paste0("Unscaled_Shift_",target),paste0("Unscaled_Break_Point_",target))])
      datasummary<-separate(data = datasummary, col = Product, into = "Product", sep = "\\|")
      
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
    
    #Resetting product counter variable 
    l=1
    if (b==5){
      master_Product_Summary<-Product_Summary
      final_master<-final
    } else {
      master_Product_Summary<-merge(master_Product_Summary,Product_Summary,by=c("Product","Store_Group"),all.x=TRUE)
      final_master<-merge(final_master,final,by=c("Store","Climate","VSG","Product","Store_Group","Space","Future_Space","Climate_Group","Productivity_Group"),all.x=TRUE)
    }
  }
  final_master<-final_master[c("Store","Climate","VSG","Product","Store_Group","Space","Sales","Scaled_Alpha_Sales","Scaled_Shift_Sales","Scaled_Beta_Sales","Scaled_BP_Sales","Units","Scaled_Alpha_Units","Scaled_Shift_Units","Scaled_Beta_Units","Scaled_BP_Units","Profit","Scaled_Alpha_Profit","Scaled_Shift_Profit","Scaled_Beta_Profit","Scaled_BP_Profit","PCT_Space_Change_Limit","Space.Lower.Limit","Space.Upper.Limit","PCT_Space_Lower_Limit","PCT_Space_Upper_Limit","Exit_Flag","Future_Space","Entry_Space","Climate_Group","Productivity_Group")]
  #treatment of Filtered data to bring it back into the main dataset
  if(is.null(filtered_final)==FALSE){
  #names(filtered_final)[which(names(filtered_final) %in% "Climate")]<-"Store_Group"
    filtered_final$Store_Group<-filtered_final$Climate
  filtered_final$Productivity_Group<-"NA"
  filtered_final$Store_Group<-paste0(filtered_final$Climate_Group,"-",filtered_final$Productivity_Group)
  #Creating scaled variables
  filtered_final$Scaled_Alpha_Sales<-0
  filtered_final$Scaled_Shift_Sales<-0
  filtered_final$Scaled_Beta_Sales<-1
  filtered_final$Scaled_BP_Sales<-0
  filtered_final$Scaled_Alpha_Units<-0
  filtered_final$Scaled_Shift_Units<-0
  filtered_final$Scaled_Beta_Units<-1
  filtered_final$Scaled_BP_Units<-0
  filtered_final$Scaled_Alpha_Profit<-0
  filtered_final$Scaled_Shift_Profit<-0
  filtered_final$Scaled_Beta_Profit<-1
  filtered_final$Scaled_BP_Profit<-0
  filtered_final$Exit_Flag<-"TRUE"
  
  for(i in 1:nrow(filtered_final)){
  if(type=="Drill_Down"){
  if(length(master_Product_Summary[which(master_Product_Summary$Product==filtered_final$Product[i] & master_Product_Summary$Store_Group==filtered_final$Store_Group[i]),]$Unscaled_Alpha_Sales)==0){
  filtered_final$Scaled_Alpha_Sales[i]<-0
  filtered_final$Scaled_Shift_Sales[i]<-0
  filtered_final$Scaled_Beta_Sales[i]<-1
  filtered_final$Scaled_BP_Sales[i]<-0
  filtered_final$Scaled_Alpha_Units[i]<-0
  filtered_final$Scaled_Shift_Units[i]<-0
  filtered_final$Scaled_Beta_Units[i]<-1
  filtered_final$Scaled_BP_Units[i]<-0
  filtered_final$Scaled_Alpha_Profit[i]<-0
  filtered_final$Scaled_Shift_Profit[i]<-0
  filtered_final$Scaled_Beta_Profit[i]<-1
  filtered_final$Scaled_BP_Profit[i]<-0
  filtered_final$Exit_Flag[i]<-"TRUE"
  } else if(length(master_Product_Summary[which(master_Product_Summary$Product==filtered_final$Product[i] & master_Product_Summary$Store_Group==filtered_final$Store_Group[i]),]$Unscaled_Alpha_Sales)!=0){
    Avg_productivity<-sum(big_master_data[which(big_master_data$Store_Group %in% filtered_final$Store_Group[i]),]$Sales)/sum(big_master_data[which(big_master_data$Store_Group %in% filtered_final$Store_Group[i]),]$Space)
    PCT_Share_Coefficient<-(sum(big_master_data[which(big_master_data$Store %in% filtered_final$Store[i]),]$Sales)/sum(big_master_data[which(big_master_data$Store %in% filtered_final$Store[i]),]$Space))/Avg_productivity
    filtered_final$Scaled_Alpha_Sales[i]<-PCT_Share_Coefficient*master_Product_Summary[which(master_Product_Summary$Product==filtered_final$Product[i] & master_Product_Summary$Store_Group==filtered_final$Store_Group[i]),]$Unscaled_Alpha_Sales
    filtered_final$Scaled_Shift_Sales[i]<-PCT_Share_Coefficient*master_Product_Summary[which(master_Product_Summary$Product==filtered_final$Product[i] & master_Product_Summary$Store_Group==filtered_final$Store_Group[i]),]$Unscaled_Shift_Sales
    filtered_final$Scaled_Beta_Sales[i]<-PCT_Share_Coefficient*master_Product_Summary[which(master_Product_Summary$Product==filtered_final$Product[i] & master_Product_Summary$Store_Group==filtered_final$Store_Group[i]),]$Unscaled_Beta_Sales
    filtered_final$Scaled_BP_Sales[i]<-PCT_Share_Coefficient*master_Product_Summary[which(master_Product_Summary$Product==filtered_final$Product[i] & master_Product_Summary$Store_Group==filtered_final$Store_Group[i]),]$Unscaled_Break_Point_Sales
    filtered_final$Scaled_Alpha_Units[i]<-PCT_Share_Coefficient*master_Product_Summary[which(master_Product_Summary$Product==filtered_final$Product[i] & master_Product_Summary$Store_Group==filtered_final$Store_Group[i]),]$Unscaled_Alpha_Units
    filtered_final$Scaled_Shift_Units[i]<-PCT_Share_Coefficient*master_Product_Summary[which(master_Product_Summary$Product==filtered_final$Product[i] & master_Product_Summary$Store_Group==filtered_final$Store_Group[i]),]$Unscaled_Shift_Units
    filtered_final$Scaled_Beta_Units[i]<-PCT_Share_Coefficient*master_Product_Summary[which(master_Product_Summary$Product==filtered_final$Product[i] & master_Product_Summary$Store_Group==filtered_final$Store_Group[i]),]$Unscaled_Beta_Units
    filtered_final$Scaled_BP_Units[i]<-PCT_Share_Coefficient*master_Product_Summary[which(master_Product_Summary$Product==filtered_final$Product[i] & master_Product_Summary$Store_Group==filtered_final$Store_Group[i]),]$Unscaled_Break_Point_Units
    filtered_final$Scaled_Alpha_Profit[i]<-PCT_Share_Coefficient*master_Product_Summary[which(master_Product_Summary$Product==filtered_final$Product[i] & master_Product_Summary$Store_Group==filtered_final$Store_Group[i]),]$Unscaled_Alpha_Profit
    filtered_final$Scaled_Shift_Profit[i]<-PCT_Share_Coefficient*master_Product_Summary[which(master_Product_Summary$Product==filtered_final$Product[i] & master_Product_Summary$Store_Group==filtered_final$Store_Group[i]),]$Unscaled_Shift_Profit
    filtered_final$Scaled_Beta_Profit[i]<-PCT_Share_Coefficient*master_Product_Summary[which(master_Product_Summary$Product==filtered_final$Product[i] & master_Product_Summary$Store_Group==filtered_final$Store_Group[i]),]$Unscaled_Beta_Profit
    filtered_final$Scaled_BP_Profit[i]<-PCT_Share_Coefficient*master_Product_Summary[which(master_Product_Summary$Product==filtered_final$Product[i] & master_Product_Summary$Store_Group==filtered_final$Store_Group[i]),]$Unscaled_Break_Point_Profit
    filtered_final$Exit_Flag[i]<-0
  } 
  } else if(type=="Tiered") {
    filtered_final$Scaled_Alpha_Sales[i]<-0
    filtered_final$Scaled_Shift_Sales[i]<-0
    filtered_final$Scaled_Beta_Sales[i]<-1
    filtered_final$Scaled_BP_Sales[i]<-0
    filtered_final$Scaled_Alpha_Units[i]<-0
    filtered_final$Scaled_Shift_Units[i]<-0
    filtered_final$Scaled_Beta_Units[i]<-1
    filtered_final$Scaled_BP_Units[i]<-0
    filtered_final$Scaled_Alpha_Profit[i]<-0
    filtered_final$Scaled_Shift_Profit[i]<-0
    filtered_final$Scaled_Beta_Profit[i]<-1
    filtered_final$Scaled_BP_Profit[i]<-0
    filtered_final$Exit_Flag[i]<-"TRUE"
  }
  }
  
  
  filtered_final<-filtered_final[c("Store","Climate","VSG","Product","Store_Group","Space","Sales","Scaled_Alpha_Sales","Scaled_Shift_Sales","Scaled_Beta_Sales","Scaled_BP_Sales","Units","Scaled_Alpha_Units","Scaled_Shift_Units","Scaled_Beta_Units","Scaled_BP_Units","Profit","Scaled_Alpha_Profit","Scaled_Shift_Profit","Scaled_Beta_Profit","Scaled_BP_Profit","PCT_Space_Change_Limit","Space.Lower.Limit","Space.Upper.Limit","PCT_Space_Lower_Limit","PCT_Space_Upper_Limit","Exit_Flag","Future_Space","Entry_Space","Climate_Group","Productivity_Group")]
  #merging filtered data with the final data
  final_master<-rbind(final_master,filtered_final)
  }
  #Final lower and upper limits variable generation
  if(type=="Tiered"){
  final_master$PCT_Change_Lower_Limit<-floor(final_master$Space*(1-final_master$PCT_Space_Change_Limit)/Increment_Size)*Increment_Size
  final_master$PCT_Change_Upper_Limit<-ceiling(final_master$Space*(1+final_master$PCT_Space_Change_Limit)/Increment_Size)*Increment_Size
  } else if(type=="Drill_Down" || is.null(PCT_Space_Change_Limit)==TRUE) {
    final_master$PCT_Change_Lower_Limit<-0
    final_master$PCT_Change_Upper_Limit<-0
  }
  final_master$PCT_of_Space_Lower_Limit<-floor(final_master$PCT_Space_Lower_Limit*(final_master$Future_Space-final_master$Entry_Space)/Increment_Size)*Increment_Size
  final_master$PCT_of_Space_Upper_Limit<-ceiling(final_master$PCT_Space_Upper_Limit*(final_master$Future_Space-final_master$Entry_Space)/Increment_Size)*Increment_Size
  
  
  for(h in (1:nrow(final_master))){
  final_master$Lower_Limit[h]<-ifelse(final_master$Exit_Flag[h]=="TRUE",0,max(final_master$Space.Lower.Limit[h],final_master$PCT_Change_Lower_Limit[h],final_master$PCT_of_Space_Lower_Limit[h]))  
  upper_limit<-c(final_master$Space.Upper.Limit[h],final_master$PCT_Change_Upper_Limit[h],final_master$PCT_of_Space_Upper_Limit[h])
  final_master$Upper_Limit[h]<-ifelse(final_master$Exit_Flag[h]=="TRUE",0,min(upper_limit[which(upper_limit!=0)]))
  }
  final_master<-final_master[c("Store","Climate","VSG","Product","Store_Group","Space","Sales","Scaled_Alpha_Sales","Scaled_Shift_Sales","Scaled_Beta_Sales","Scaled_BP_Sales","Units","Scaled_Alpha_Units","Scaled_Shift_Units","Scaled_Beta_Units","Scaled_BP_Units","Profit","Scaled_Alpha_Profit","Scaled_Shift_Profit","Scaled_Beta_Profit","Scaled_BP_Profit","Lower_Limit","Upper_Limit")]
  final_out<-list(master_Product_Summary,final_master)
  return(final_out)
  }
#   
  #Function calling
  output<-curvefitting_boundsetting(big_master_data,bound_input,Increment_Size,sales_weight,profit_weight,units_weight,PCT_Space_Change_Limit,type)
  #Final output
  write.csv(output[1], "Analytics_Reference_Data.csv",row.names=FALSE)
  write.csv(output[2], "Output_Data.csv",row.names=FALSE)
  