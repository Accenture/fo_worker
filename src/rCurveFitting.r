  #Clear historical data from R server
#  rm(list=ls())

  #install this package to run erf() function
  library(pracma)
  #Data manipulation task performed using tidyr package
  library(tidyr)
  #Library to call optimization package that runs the curve fitting
  library(nloptr)
  library(gdata)
#  library(xlsx)

  #initial parameter setting for curve fitting
#  Increment_Size=2.5

  #Bound parameters
#  PCT_Space_Change_Limit<-0.5
  #sales penetration threshold
#  sales_penetration_threshold<-0.01
  #Select Optimization type Tiered or Drill_Down
#  jobType<-"Tiered"
  #Optimization methodology parameter which can take on values of Traditional or Enhanced
#  methodology<-"Traditional"
  #big_master_data$Trgt_BA_Space_Less_Brnd_Entry<-as.numeric(as.character(big_master_data$Future_Space))-big_master_data$Entry_Space
#  big_master_data<-read.csv("output.csv",header=TRUE,sep=",")
#  bound_input<-read.csv("Bound_Input1.csv",header=TRUE,sep=",")

  #Curve Fitting and Bound Setting Function design
curvefitting_boundsetting<-function(big_master_data,bound_input,Increment_Size,sales_weight,profit_weight,units_weight,PCT_Space_Change_Limit,sales_penetration_threshold,jobType,methodology){
    library(pracma)
    library(tidyr)
    library(nloptr)
    library(gdata)
    print(bound_input)

    #Parameters for Filtering store-product combination where there is too little volume/store count to build a curve
    strcount_filter=100
    avgsales_filter=200
    avgprofit_filter=50
    avgunits_filter=50
    curve_split<-0.75
    #Parameters for Filtering store/product where sales<20$, profit<$5,units<5 or Space<0.1
    space_filter=0.1
    sales_filter=20
    profit_filter=5
    units_filter=5

  #bound percent variable creation
    names(bound_input)[1]<-"Category"
    names(big_master_data)[c(12,13,14,15,16,17)]<-c("BOH_Dollar","BOH_Units","CC_Count_w_BOH","Profit_Percent","Receipts_Dollar","Receipts_Units")
    big_master_data<-big_master_data[,which(names(big_master_data) %in% c("Store","Climate","VSG","Category","Space","Sales","Units","Profit","Exit_Flag","Future_Space","Entry_Space","BOH_Dollar","BOH_Units","CC_Count_w_BOH","Profit_Percent","Receipts_Dollar","Receipts_Units"))]
    big_master_data$PCT_Space_Change_Limit<-PCT_Space_Change_Limit
    bound_input$Category<-trim(bound_input$Category)
    big_master_data<-merge(big_master_data,bound_input,by="Category",all.x=TRUE)

  #create "Climate_Group" variable
    big_master_data$Climate_Group<-big_master_data$Climate
    big_master_data$Climate_Group<-as.character(big_master_data$Climate_Group)
    if(jobType=="Drill_Down"){
    for(j in 1:nrow(big_master_data)){
      big_master_data$Climate_Group[j]<-ifelse((big_master_data$Climate_Group[j]=="HOT" || big_master_data$Climate_Group[j]=="SUPER HOT"),"HOT & SH",big_master_data$Climate_Group[j])
    }
    } else {
    big_master_data$Climate_Group<-"NA"
    }

  #Filter out product/climate group combinations
  big_master_data$prod_climate<-paste0(big_master_data$Category,"-",big_master_data$Climate_Group)
  big_master_data$flag<-0
  filtered_final<-NULL
  #Space filter if Space<space_filter
  p=1
  if (length(which(big_master_data$Space<space_filter))!=0){
    if(p==1){
      filtered_data<-big_master_data[which(big_master_data$Space<space_filter),]
      filtered_final<-filtered_data
      p=p+1
    } else {
      filtered_data<-big_master_data[which(big_master_data$Space<space_filter),]
      filtered_final<-rbind(filtered_final,filtered_data)
    }
    big_master_data<-big_master_data[-which(big_master_data$Space<space_filter),]
  }

  #Filter store-product combination where there is too little volume/store count to build a curve
  productlist<-unique(big_master_data$prod_climate)
  for (i in (1:length(productlist))){
    testdata<-big_master_data[which(big_master_data$prod_climate %in% productlist[i]),]
    if(nrow(testdata)>strcount_filter && ifelse(is.na(avgsales_filter)!=TRUE ,sum(testdata$Sales)/nrow(testdata)>avgsales_filter,TRUE) && ifelse(is.na(avgprofit_filter)!=TRUE ,sum(testdata$Profit)/nrow(testdata)>avgprofit_filter,TRUE) && ifelse(is.na(avgunits_filter)!=TRUE ,sum(testdata$Units)/nrow(testdata)>avgunits_filter,TRUE)){
      big_master_data<-big_master_data
    } else {
      #storing filtered data for later use
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
  big_master_data<-big_master_data[c("Category","Store","VSG","Space","Sales","Units","Profit","Exit_Flag","Future_Space","Entry_Space", "BOH_Dollar","BOH_Units","CC_Count_w_BOH","Profit_Percent","Receipts_Dollar","Receipts_Units","PCT_Space_Change_Limit","Space.Lower.Limit","Space.Upper.Limit","PCT_Space_Lower_Limit","PCT_Space_Upper_Limit","Climate_Group","prod_climate","flag","Climate")]
  #b=5
  #loop to filter Store, Product, Space or specific VPE variables to run curve fitting
  if(nrow(big_master_data)!=0){
  for(b in (5:7)){
    if("Profit" %in% names(big_master_data)[b]){
      target<-"Profit"
      metric_filter<-profit_filter
    } else if("Sales" %in% names(big_master_data)[b]){
      target<-"Sales"
      metric_filter<-sales_filter
    } else if("Units" %in% names(big_master_data)[b]){
      target<-"Units"
      metric_filter<-units_filter
    }
    big_master_data$flag<-0
  #Filter store/product where sales<sales_filter$, profit<profit_filter$ and units<units_filter$
  if(length(which(big_master_data[,target]<metric_filter)!=0)){
      big_master_data$flag[which(big_master_data[,target]<metric_filter)]=1
  }

  #Productivity Group preparation based on different rules
  #j=2
  for(j in 1:nrow(big_master_data)){
    big_master_data$productivity_group_dummy[j]<-sum(big_master_data[,target][which(big_master_data$Store %in% big_master_data$Store[j])])/sum(big_master_data$Space[which(big_master_data$Store %in% big_master_data$Store[j])])
  }
  big_master_data$str_climate<-paste0(big_master_data$Store,"-",big_master_data$Climate_Group)
  big_master_data[,paste0("Productivity_Group_",target)]<-NA
  for(q in (1:nrow(big_master_data))){
    big_master_data_backup<-big_master_data[which(big_master_data$Climate_Group %in% big_master_data$Climate_Group[q]),]
    big_master_data_backup<-big_master_data_backup[which(big_master_data_backup$Category %in% big_master_data$Category[q]),]
    if(nrow(big_master_data_backup)>=600){
      big_master_data[,paste0("Productivity_Group_",target)][q]<-ifelse(big_master_data$productivity_group_dummy[q]<quantile(big_master_data_backup$productivity_group_dummy,0.25),"Low",ifelse(big_master_data$productivity_group_dummy[q]>quantile(big_master_data_backup$productivity_group_dummy,0.75),"High","Medium"))
    } else if (nrow(big_master_data_backup)>=400){
      big_master_data[,paste0("Productivity_Group_",target)][q]<-ifelse(big_master_data$productivity_group_dummy[q]<quantile(big_master_data_backup$productivity_group_dummy,0.5),"Low","High")
    } else {
      big_master_data[,paste0("Productivity_Group_",target)][q]<-"NA"
    }
  }
  big_master_data[paste0("Store_Group_",target)]<-paste0(big_master_data$Climate_Group,"-",big_master_data[,paste0("Productivity_Group_",target)])
  big_master_data<-big_master_data[,-which(names(big_master_data) %in% c("productivity_group_dummy","str_climate"))]
  big_master_data<-big_master_data[c("Store","Category",paste0("Store_Group_",target),"Space","Sales","Profit","Units","PCT_Space_Change_Limit","Space.Lower.Limit","Space.Upper.Limit","PCT_Space_Lower_Limit","PCT_Space_Upper_Limit","Exit_Flag","Future_Space","Entry_Space","Climate_Group",paste0("Productivity_Group_",target),"Climate","VSG","BOH_Dollar","BOH_Units","CC_Count_w_BOH","Profit_Percent","Receipts_Dollar","Receipts_Units","flag")]
  big_master_data$Category<-paste0(big_master_data$Category,"|",big_master_data[,paste0("Store_Group_",target)])

    master_data<-big_master_data[,c(1:4,b,8:26)]
    #master_data<-big_master_data[,c(which(names(big_master_data) %in% c("Store", "Category")),b,8:19)]
    #sum of total space allocated across products for a given store and Break Point variable creation
    produclist<-unique(master_data$Category)
    for(j in 1:nrow(master_data)){
      master_data$BA_Space[j]<-sum(master_data$Space[which(master_data$Store == master_data$Store[j])])
    }

    for(j in 1:nrow(master_data)){
      master_data[[paste0("Unscaled_Break_Point_",target)]][j]<-quantile(master_data$Space[which(master_data$Category %in% master_data$Category[j])],0.01)
    }

    #Variable to control position of the Curve plot output in an excel file
    # Loop for Data filter by product
    l=1
    #k=1
    #loop to run Curve fitting by product and store
    for (k in 1:nrow(data.frame(produclist))){
     #for (k in 1:2){
      data<-master_data[which(master_data$Category %in% produclist[k]),]
      #data1<-data[which(data$flag==0),]
      #Space_backup <- as.vector(1:nrow(data))
      if(length(unique(data$Space[which(data$flag==0)]))<=1){
        data[,paste0("Correlation_",target)]<-NA
        Estimate<-c(mean(data[which(data$flag==0),target]),-((unique(data$Space[which(data$flag==0)])/2)-0)/qnorm((1-(mean(data[which(data$flag==0),target])/2)/mean(data[which(data$flag==0),target]))/2),0)
        coef<-data.frame(Estimate)
        coef<-data.frame(t(data.frame(coef)))
        colnames(coef)<-c("Alpha","Beta","Shift")
        data[which(data$flag==0),paste0("Alpha_Seed_",target)]<-NA
        data[which(data$flag==0),paste0("Shift_Seed_",target)]<-NA
        data[which(data$flag==0),paste0("Beta_Seed_",target)]<-NA
        data[which(data$flag==0),paste0("Alpha_UB_",target)]<-NA
        data[which(data$flag==0),paste0("Beta_UB_",target)]<-NA
        data[which(data$flag==0),paste0("Shift_UB_",target)]<-NA

      } else {

      #starting values for curve fitting coefficients
      data[which(data$flag==0),paste0("Correlation_",target)]<-cor(data[which(data$flag==0),c(target,"Space")])[1,2]
      data[which(data$flag==0),paste0("Alpha_Seed_",target)]<-mean(data[which(data$flag==0),target][which(data$Space[which(data$flag==0)]>=quantile(data$Space[which(data$flag==0)], curve_split))])
      if(unique(data[which(data$flag==0),paste0("Alpha_Seed_",target)])<0){
        data[which(data$flag==0),paste0("Alpha_Seed_",target)]<-1
      }
      spacetosolvefor<-mean(data$Space[which(data$Space[which(data$flag==0)]<=quantile(data$Space[which(data$flag==0)], 1-curve_split))])
      salestosolvefor<-min(unique(data[which(data$flag==0),paste0("Alpha_Seed_",target)]), mean(data[,target][which(data$Space[which(data$flag==0)]<=quantile(data$Space[which(data$flag==0)], 1-curve_split))]))
      data[which(data$flag==0),paste0("Shift_Seed_",target)]<-0

      data[which(data$flag==0),paste0("Beta_Seed_",target)]<--(spacetosolvefor-unique(data[which(data$flag==0),paste0("Shift_Seed_",target)]))/qnorm((1-salestosolvefor/unique(data[which(data$flag==0),paste0("Alpha_Seed_",target)]))/2)
      if(unique(data[which(data$flag==0),paste0("Beta_Seed_",target)])<0 || is.na(unique(data[which(data$flag==0),paste0("Beta_Seed_",target)]))==TRUE){
        data[which(data$flag==0),paste0("Beta_Seed_",target)]<-0
      }
      #Lower bounds for curve fitting coefficients
      Alpha_LB<-0
      Beta_LB<-0
      Shift_LB<-0
      #Upper bounds for curve fitting coefficients
      data[which(data$flag==0),paste0("Alpha_UB_",target)]<-quantile(data[which(data$flag==0),target], 0.95)
      data[which(data$flag==0),paste0("Beta_UB_",target)]<-Inf
      if(min(data$Space[which(data$flag==0)])<=1){
        data[which(data$flag==0),paste0("Shift_UB_",target)]<-0
      } else {
        data[which(data$flag==0),paste0("Shift_UB_",target)]<-min(data$Space[which(data$flag==0)])-1
      }

      #defining Sales-Space functions for optimization
      Space<-data$Space[which(data$flag==0)]
      targetset<-data[which(data$flag==0),target]
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
      x0<-c(unique(data[which(data$flag==0),paste0("Alpha_Seed_",target)]),unique(data[which(data$flag==0),paste0("Beta_Seed_",target)]),unique(data[which(data$flag==0),paste0("Shift_Seed_",target)]))
      #Calling optimization function "auglag"
      model<-auglag(x0, predfun,gr=NULL,
             lower=c(Alpha_LB,Beta_LB,Shift_LB),
             upper=c(data.frame(data[which(data$flag==0),paste0("Alpha_UB_",target)])[1,1],Inf,unique(data[which(data$flag==0),paste0("Shift_UB_",target)])),
             localsolver = c("MMA"))

      #Extracting model results
      coef<-t(data.frame(model$par))
      }
      coef[,c(1,2,3)]<-round(coef[,c(1,2,3)],4)
      if(unique(data[which(data$flag==0),paste0("Correlation_",target)])<=0.2 && is.na(unique(data[which(data$flag==0),paste0("Correlation_",target)]))==FALSE){
        Estimate<-c(mean(data[which(data$flag==0),target]),min(data$Space[which(data$flag==0)])/(sqrt(2)*qnorm(mean(c(mean(data[which(data$flag==0),target]),(sum(data[which(data$flag==0),target])/sum(data$Space[which(data$flag==0)]))*min(data$Space[which(data$flag==0)])))/mean(data[which(data$flag==0),target]))),0)
        coef<-data.frame(dummy=1,Estimate)
        rownames(coef)<-c("Alpha","Beta","Shift")
        coef<-coef[,-1]
        #coef<-t(data.frame(coef))
      }
      Prediction<-coef[1]*erf((data$Space[which(data$flag==0)]-coef[3])/(sqrt(2)*coef[2]))

      #quasi rsquared value is calculated assuming nonlinear function to be reasonably close to a linear model
      data[which(data$flag==0),paste0("Quasi_R_Squared_",target)]<-1-sum((data[which(data$flag==0),target]-Prediction)^2)/(length(data[which(data$flag==0),target])*var(data[which(data$flag==0),target]))

      #Mean Absolute Percentage Error(MAPE) calculation
      data[which(data$flag==0),paste0("MAPE_",target)]<-mean(abs(Prediction-(data[which(data$flag==0),target]))/(data[which(data$flag==0),target]))

      #Generating unscalled and scaled variables by store
      data[which(data$flag==0),paste0("Unscaled_Alpha_",target)]<-coef[1]
      data[which(data$flag==0),paste0("Unscaled_Beta_",target)]<-coef[2]
      data[which(data$flag==0),paste0("Unscaled_Shift_",target)]<-coef[3]
      #data[,paste0("Unscaled_Shift_",target)]<-ifelse(coef[3]>0 && coef[3]<1,round(coef[3],0),coef[3])

      data[which(data$flag==0),paste0("Unscaled_Break_Point_",target)]<-ifelse(data[which(data$flag==0),paste0("Unscaled_Shift_",target)]==0,0,data[which(data$flag==0),paste0("Unscaled_Break_Point_",target)])
      data[which(data$flag==0),paste0("Productivity_",target)]<-data[which(data$flag==0),target]/data$Space[which(data$flag==0)]
      data[which(data$flag==0),paste0(target,"_BP")]<-data[which(data$flag==0),paste0("Unscaled_Alpha_",target)]*erf((data[which(data$flag==0),paste0("Unscaled_Break_Point_",target)]-data[which(data$flag==0),paste0("Unscaled_Shift_",target)])/(sqrt(2)*data[which(data$flag==0),paste0("Unscaled_Beta_",target)]))
      data[which(data$flag==0),paste0("Productivity_BP",target)]<-data[which(data$flag==0),paste0(target,"_BP")]/data[which(data$flag==0),paste0("Unscaled_Break_Point_",target)]
      data[which(data$flag==0),paste0("Predicted_",target)]<-ifelse(data$Space[which(data$flag==0)]<data[which(data$flag==0),paste0("Unscaled_Break_Point_",target)],data$Space[which(data$flag==0)]*data[which(data$flag==0),paste0("Productivity_BP",target)],data[which(data$flag==0),paste0("Unscaled_Alpha_",target)]*erf((data$Space[which(data$flag==0)]-data[which(data$flag==0),paste0("Unscaled_Shift_",target)])/(sqrt(2)*data[which(data$flag==0),paste0("Unscaled_Beta_",target)])))
      data[which(data$flag==0),paste0("Scaling_Type_",target)]<-ifelse(data[which(data$flag==0),target]>=data[which(data$flag==0),paste0("Predicted_",target)],ifelse(data[which(data$flag==0),target]>=data[which(data$flag==0),paste0(target,"_BP")],"A","C"),ifelse(data$Space[which(data$flag==0)]>=data[which(data$flag==0),paste0("Unscaled_Break_Point_",target)],"B","D"))

      data[which(data$flag==0),paste0("Space2Solve4_",target)]<-ifelse(data[which(data$flag==0),paste0("Scaling_Type_",target)]=="A" | data[which(data$flag==0),paste0("Scaling_Type_",target)]=="B",data$Space[which(data$flag==0)],ifelse(data[which(data$flag==0),paste0("Scaling_Type_",target)]=="D",data[which(data$flag==0),paste0("Unscaled_Break_Point_",target)],data[which(data$flag==0),paste0(target,"_BP")]/data[which(data$flag==0),paste0("Productivity_",target)]))
      data[which(data$flag==0),paste0(target,"2Solve4")]<-ifelse(data[which(data$flag==0),paste0("Scaling_Type_",target)]=="A" | data[which(data$flag==0),paste0("Scaling_Type_",target)]=="B",data[which(data$flag==0),target],ifelse(data[which(data$flag==0),paste0("Scaling_Type_",target)]=="D",data[which(data$flag==0),paste0("Unscaled_Break_Point_",target)]*data[which(data$flag==0),paste0("Productivity_",target)],data[which(data$flag==0),paste0(target,"_BP")]))
      data[which(data$flag==0),paste0("Scaled_Alpha_",target)]<-0
      data[which(data$flag==0),paste0("Scaled_Shift_",target)]<-0
      data[which(data$flag==0),paste0("Scaled_Beta_",target)]<-0
      data[which(data$flag==0),paste0("Scaled_BP_",target)]<-0

      for(m in 1:nrow(data)){
      critical_space<-data[,paste0("Unscaled_Shift_",target)][m]+sqrt(2)*data[,paste0("Unscaled_Beta_",target)][m]*sqrt(log(sqrt(7/11)*(data[,paste0("Unscaled_Alpha_",target)][m]/data[,paste0("Unscaled_Beta_",target)][m])))
      data[,paste0("Scaled_Alpha_",target)][m]<-data[,paste0("Unscaled_Alpha_",target)][m]+data[,target][m]-data[,paste0("Predicted_",target)][m]
      data[,paste0("Scaled_Shift_",target)][m]<-ifelse(data$Space[m]>critical_space,coef[3],ifelse(data[,paste0("Scaling_Type_",target)][m]=="B" | data[,paste0("Scaling_Type_",target)][m]=="D",data[,paste0("Unscaled_Shift_",target)][m],ifelse(data[,paste0("Space2Solve4_",target)][m]+data[,paste0("Unscaled_Beta_",target)][m]*qnorm((1-(data[,paste0(target,"2Solve4")][m]/data[,paste0("Scaled_Alpha_",target)][m]))/2)>0,data[,paste0("Space2Solve4_",target)][m]+data[,paste0("Unscaled_Beta_",target)][m]*qnorm((1-(data[,paste0(target,"2Solve4")][m]/data[,paste0("Scaled_Alpha_",target)][m]))/2),0)))
      data[,paste0("Scaled_Beta_",target)][m]<-ifelse(data$Space[m]>critical_space,coef[2],(data[,paste0("Scaled_Shift_",target)][m]-data[,paste0("Space2Solve4_",target)][m])/(qnorm((1-(data[,paste0(target,"2Solve4")][m]/data[,paste0("Scaled_Alpha_",target)][m]))/2)))
      data[,paste0("Scaled_BP_",target)][m]<-ifelse(data$Space[m]>critical_space,data[,paste0("Unscaled_Break_Point_",target)][m],ifelse(!data[,paste0("Scaling_Type_",target)][m]=="A",data[,paste0("Unscaled_Break_Point_",target)][m],data[,paste0("Scaled_Shift_",target)][m]-data[,paste0("Scaled_Beta_",target)][m]*(qnorm((1-(data[,paste0(target,"_BP")][m]/data[,paste0("Scaled_Alpha_",target)][m]))/2))))
      }
      data[which(data$flag==0),paste0(target,"Scaled_BP")]<- data[which(data$flag==0),paste0("Scaled_Alpha_",target)]*erf((data[which(data$flag==0),paste0("Scaled_BP_",target)]-data[which(data$flag==0),paste0("Scaled_Shift_",target)])/(sqrt(2)*data[which(data$flag==0),paste0("Scaled_Beta_",target)]))
      data[which(data$flag==0),paste0("Productivity_ScaledBP_",target)]<-data[which(data$flag==0),paste0(target,"Scaled_BP")]/data[which(data$flag==0),paste0("Scaled_BP_",target)]
      data[which(data$flag==0),paste0("Predicted_",target,"_Final")]<-ifelse(data$Space[which(data$flag==0)]<data[which(data$flag==0),paste0("Scaled_BP_",target)],data$Space[which(data$flag==0)]*data[which(data$flag==0),paste0("Productivity_ScaledBP_",target)],data[which(data$flag==0),paste0("Scaled_Alpha_",target)]*erf((data$Space[which(data$flag==0)]-data[which(data$flag==0),paste0("Scaled_Shift_",target)])/(sqrt(2)*data[which(data$flag==0),paste0("Scaled_Beta_",target)])))
      data<-separate(data = data, col = Category, into = "Category", sep = "\\|")

      #creating products summary with unscalled coeffcients and important model metrics
      datasummary<-unique(data[which(data$flag==0),c("Category",paste0("Store_Group_",target),paste0("Quasi_R_Squared_",target),paste0("MAPE_",target),paste0("Correlation_",target),paste0("Alpha_Seed_",target),paste0("Beta_Seed_",target),paste0("Shift_Seed_",target),paste0("Alpha_UB_",target),paste0("Beta_UB_",target),paste0("Shift_UB_",target),paste0("Unscaled_Alpha_",target),paste0("Unscaled_Beta_",target),paste0("Unscaled_Shift_",target),paste0("Unscaled_Break_Point_",target))])
      datasummary<-separate(data = datasummary, col = Category, into = "Category", sep = "\\|")

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

    if(length(which(final$flag==1))!=0){
      for(m in (1:nrow(final))){
        if(final$flag[m]==1){
      if(jobType=="Drill_Down"){
      if(length(Product_Summary[which(Product_Summary$Category == final$Category[m] & Product_Summary[,paste0("Store_Group_",target)] == final[,paste0("Store_Group_",target)][m]),][,paste0("Unscaled_Alpha_",target)])==0){
      final[,paste0("Scaled_Alpha_",target)][m]<-0
      final[,paste0("Scaled_Shift_",target)][m]<-0
      final[,paste0("Scaled_Beta_",target)][m]<-1
      final[,paste0("Scaled_BP_",target)][m]<-0
      if(methodology=="Traditional"){
        final$Exit_Flag[m]==0
      } else {
        final$Exit_Flag[m]<-"Exit"
      }
      } else if(length(Product_Summary[which(Product_Summary$Category == final$Category[m] & Product_Summary[,paste0("Store_Group_",target)] == final[,paste0("Store_Group_",target)][m]),][,paste0("Unscaled_Alpha_",target)])!=0){
        Avg_productivity<-sum(final[which(final[,paste0("Store_Group_",target)] %in% final[,paste0("Store_Group_",target)][m] & final$flag==0),][,target])/sum(final[which(final[,paste0("Store_Group_",target)] %in% final[,paste0("Store_Group_",target)][m]  & final$flag==0),]$Space)
        PCT_Share_Coefficient<-(sum(final[which(final$Store %in% final$Store[m]  & final$flag==0),][,target])/sum(final[which(final$Store %in% final$Store[m]  & final$flag==0),]$Space))/Avg_productivity
        final[,paste0("Scaled_Alpha_",target)][m]<-PCT_Share_Coefficient*Product_Summary[which(Product_Summary$Category==final$Category[m] & Product_Summary[,paste0("Store_Group_",target)]==final[,paste0("Store_Group_",target)][m]),][,paste0("Unscaled_Alpha_",target)]
        final[,paste0("Scaled_Shift_",target)][m]<-PCT_Share_Coefficient*Product_Summary[which(Product_Summary$Category==final$Category[m] & Product_Summary[,paste0("Store_Group_",target)]==final[,paste0("Store_Group_",target)][m]),][,paste0("Unscaled_Shift_",target)]
        final[,paste0("Scaled_Beta_",target)][m]<-PCT_Share_Coefficient*Product_Summary[which(Product_Summary$Category==final$Category[m] & Product_Summary[,paste0("Store_Group_",target)]==final[,paste0("Store_Group_",target)][m]),][,paste0("Unscaled_Beta_",target)]
        final[,paste0("Scaled_BP_",target)][m]<-PCT_Share_Coefficient*Product_Summary[which(Product_Summary$Category==final$Category[m] & Product_Summary[,paste0("Store_Group_",target)]==final[,paste0("Store_Group_",target)][m]),][,paste0("Unscaled_Break_Point_",target)]
        if(final$Exit_Flag[m]=="Exit"){
          final$Exit_Flag[m]<-"Exit"
        } else {
          final$Exit_Flag[m]<-0
        }
      }
      } else {
        final[,paste0("Scaled_Alpha_",target)][m]<-0
        final[,paste0("Scaled_Shift_",target)][m]<-0
        final[,paste0("Scaled_Beta_",target)][m]<-1
        final[,paste0("Scaled_BP_",target)][m]<-0
        if(methodology=="Traditional"){
          final$Exit_Flag[m]==0
        } else {
          final$Exit_Flag[m]<-"Exit"
        }
    }
    }
      }
    }

    l=1
    if (b==5){
      #master_Product_Summary<-Product_Summary
      final_master<-final
      master_Product_Summary<-list()
    } else {
      #master_Product_Summary<-cbind(master_Product_Summary,Product_Summary)
      final_master<-merge(final_master,final,by=c("Store","Climate","VSG","Category","Space","Future_Space","Climate_Group","BOH_Dollar","BOH_Units","CC_Count_w_BOH","Profit_Percent","Receipts_Dollar","Receipts_Units"),all.x=TRUE)
    }

    master_Product_Summary[[b]]<-Product_Summary
  }
    final_master<-final_master[c("Store","Climate","VSG","Category","Store_Group_Sales","Store_Group_Profit","Store_Group_Units","Space","Sales","Scaled_Alpha_Sales","Scaled_Shift_Sales","Scaled_Beta_Sales","Scaled_BP_Sales","Units","Scaled_Alpha_Units","Scaled_Shift_Units","Scaled_Beta_Units","Scaled_BP_Units","Profit","Scaled_Alpha_Profit","Scaled_Shift_Profit","Scaled_Beta_Profit","Scaled_BP_Profit","PCT_Space_Change_Limit","Space.Lower.Limit","Space.Upper.Limit","PCT_Space_Lower_Limit","PCT_Space_Upper_Limit","Exit_Flag","Future_Space","Entry_Space","Climate_Group","Productivity_Group_Sales","Productivity_Group_Profit","Productivity_Group_Units","BOH_Dollar","BOH_Units","CC_Count_w_BOH","Profit_Percent","Receipts_Dollar","Receipts_Units")]
  }

  #treatment of Filtered data to bring it back into the main dataset
  if(is.null(filtered_final)==FALSE){

    #names(filtered_final)[which(names(filtered_final) %in% "Climate")]<-"Store_Group"
    filtered_final$Store_Group<-filtered_final$Climate
    filtered_final$Productivity_Group_Sales<-"NA"
    filtered_final$Productivity_Group_Profit<-"NA"
    filtered_final$Productivity_Group_Units<-"NA"
    filtered_final$Store_Group_Sales<-paste0(filtered_final$Climate_Group,"-",filtered_final$Productivity_Group_Sales)
    filtered_final$Store_Group_Profit<-paste0(filtered_final$Climate_Group,"-",filtered_final$Productivity_Group_Profit)
    filtered_final$Store_Group_Units<-paste0(filtered_final$Climate_Group,"-",filtered_final$Productivity_Group_Units)
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
    i=1
    for(i in 1:nrow(filtered_final)){
      if(jobType=="Drill_Down" && nrow(big_master_data)!=0){
        if(length(master_Product_Summary[[5]][which(master_Product_Summary[[5]]$Category == filtered_final$Category[i] & master_Product_Summary[[5]]$Store_Group_Sales == filtered_final$Store_Group_Sales[i]),]$Unscaled_Alpha_Sales)==0){
          filtered_final$Scaled_Alpha_Sales[i]<-0
          filtered_final$Scaled_Shift_Sales[i]<-0
          filtered_final$Scaled_Beta_Sales[i]<-1
          filtered_final$Scaled_BP_Sales[i]<-0
          if(methodology=="Traditional"){
            filtered_final$Exit_Flag[i]==0
          } else {
            filtered_final$Exit_Flag[i]<-"Exit"
          }
        } else if(length(master_Product_Summary[[5]][which(master_Product_Summary[[5]]$Category == filtered_final$Category[i] & master_Product_Summary[[5]]$Store_Group_Sales == filtered_final$Store_Group_Sales[i]),]$Unscaled_Alpha_Sales)!=0){
          Avg_productivity<-sum(final_master[which(final_master$Store_Group_Sales %in% filtered_final$Store_Group_Sales[i]),]$Sales)/sum(final_master[which(final_master$Store_Group_Sales %in% filtered_final$Store_Group_Sales[i]),]$Space)
          PCT_Share_Coefficient<-(sum(final_master[which(final_master$Store %in% filtered_final$Store[i]),]$Sales)/sum(final_master[which(final_master$Store %in% filtered_final$Store[i]),]$Space))/Avg_productivity
          filtered_final$Scaled_Alpha_Sales[i]<-PCT_Share_Coefficient*master_Product_Summary[[5]][which(master_Product_Summary[[5]]$Category==filtered_final$Category[i] & master_Product_Summary[[5]]$Store_Group_Sales==filtered_final$Store_Group_Sales[i]),]$Unscaled_Alpha_Sales
          filtered_final$Scaled_Shift_Sales[i]<-PCT_Share_Coefficient*master_Product_Summary[[5]][which(master_Product_Summary[[5]]$Category==filtered_final$Category[i] & master_Product_Summary[[5]]$Store_Group_Sales==filtered_final$Store_Group_Sales[i]),]$Unscaled_Shift_Sales
          filtered_final$Scaled_Beta_Sales[i]<-PCT_Share_Coefficient*master_Product_Summary[[5]][which(master_Product_Summary[[5]]$Category==filtered_final$Category[i] & master_Product_Summary[[5]]$Store_Group_Sales==filtered_final$Store_Group_Sales[i]),]$Unscaled_Beta_Sales
          filtered_final$Scaled_BP_Sales[i]<-PCT_Share_Coefficient*master_Product_Summary[[5]][which(master_Product_Summary[[5]]$Category==filtered_final$Category[i] & master_Product_Summary[[5]]$Store_Group_Sales==filtered_final$Store_Group_Sales[i]),]$Unscaled_Break_Point_Sales
          if(filtered_final$Exit_Flag[i]=="Exit"){
            filtered_final$Exit_Flag[i]<-"Exit"
          } else {
            filtered_final$Exit_Flag[i]<-0
          }
        }
        if(length(master_Product_Summary[[7]][which(master_Product_Summary[[7]]$Category == filtered_final$Category[i] & master_Product_Summary[[7]]$Store_Group_Units == filtered_final$Store_Group_Units[i]),]$Unscaled_Alpha_Units)==0){
          filtered_final$Scaled_Alpha_Units[i]<-0
          filtered_final$Scaled_Shift_Units[i]<-0
          filtered_final$Scaled_Beta_Units[i]<-1
          filtered_final$Scaled_BP_Units[i]<-0
          if(methodology=="Traditional"){
            filtered_final$Exit_Flag[i]==0
          } else {
            filtered_final$Exit_Flag[i]<-"Exit"
          }
        } else if(length(master_Product_Summary[[7]][which(master_Product_Summary[[7]]$Category == filtered_final$Category[i] & master_Product_Summary[[7]]$Store_Group_Units == filtered_final$Store_Group_Units[i]),]$Unscaled_Alpha_Units)!=0) {
          Avg_productivity<-sum(final_master[which(final_master$Store_Group_Units %in% filtered_final$Store_Group_Units[i]),]$Units)/sum(final_master[which(final_master$Store_Group_Units %in% filtered_final$Store_Group_Units[i]),]$Space)
          PCT_Share_Coefficient<-(sum(final_master[which(final_master$Store %in% filtered_final$Store[i]),]$Units)/sum(final_master[which(final_master$Store %in% filtered_final$Store[i]),]$Space))/Avg_productivity
          filtered_final$Scaled_Alpha_Units[i]<-PCT_Share_Coefficient*master_Product_Summary[[7]][which(master_Product_Summary[[7]]$Category==filtered_final$Category[i] & master_Product_Summary[[7]]$Store_Group_Units==filtered_final$Store_Group_Units[i]),]$Unscaled_Alpha_Units
          filtered_final$Scaled_Shift_Units[i]<-PCT_Share_Coefficient*master_Product_Summary[[7]][which(master_Product_Summary[[7]]$Category==filtered_final$Category[i] & master_Product_Summary[[7]]$Store_Group_Units==filtered_final$Store_Group_Units[i]),]$Unscaled_Shift_Units
          filtered_final$Scaled_Beta_Units[i]<-PCT_Share_Coefficient*master_Product_Summary[[7]][which(master_Product_Summary[[7]]$Category==filtered_final$Category[i] & master_Product_Summary[[7]]$Store_Group_Units==filtered_final$Store_Group_Units[i]),]$Unscaled_Beta_Units
          filtered_final$Scaled_BP_Units[i]<-PCT_Share_Coefficient*master_Product_Summary[[7]][which(master_Product_Summary[[7]]$Category==filtered_final$Category[i] & master_Product_Summary[[7]]$Store_Group_Units==filtered_final$Store_Group_Units[i]),]$Unscaled_Break_Point_Units
          if(filtered_final$Exit_Flag[i]=="Exit"){
            filtered_final$Exit_Flag[i]<-"Exit"
          } else {
            filtered_final$Exit_Flag[i]<-0
          }
        }
        if(length(master_Product_Summary[[6]][which(master_Product_Summary[[6]]$Category == filtered_final$Category[i] & master_Product_Summary[[6]]$Store_Group_Profit == filtered_final$Store_Group_Profit[i]),]$Unscaled_Alpha_Profit)==0){
          filtered_final$Scaled_Alpha_Profit[i]<-0
          filtered_final$Scaled_Shift_Profit[i]<-0
          filtered_final$Scaled_Beta_Profit[i]<-1
          filtered_final$Scaled_BP_Profit[i]<-0
          if(methodology=="Traditional"){
            filtered_final$Exit_Flag[i]==0
          } else {
            filtered_final$Exit_Flag[i]<-"Exit"
          }
        } else if(length(master_Product_Summary[[6]][which(master_Product_Summary[[6]]$Category == filtered_final$Category[i] & master_Product_Summary[[6]]$Store_Group_Profit == filtered_final$Store_Group_Profit[i]),]$Unscaled_Alpha_Profit)!=0) {
          Avg_productivity<-sum(final_master[which(final_master$Store_Group_Profit %in% filtered_final$Store_Group_Profit[i]),]$Profit)/sum(final_master[which(final_master$Store_Group_Profit %in% filtered_final$Store_Group_Profit[i]),]$Space)
          PCT_Share_Coefficient<-(sum(final_master[which(final_master$Store %in% filtered_final$Store[i]),]$Profit)/sum(final_master[which(final_master$Store %in% filtered_final$Store[i]),]$Space))/Avg_productivity
          filtered_final$Scaled_Alpha_Profit[i]<-PCT_Share_Coefficient*master_Product_Summary[[6]][which(master_Product_Summary[[6]]$Category==filtered_final$Category[i] & master_Product_Summary[[6]]$Store_Group_Profit==filtered_final$Store_Group_Profit[i]),]$Unscaled_Alpha_Profit
          filtered_final$Scaled_Shift_Profit[i]<-PCT_Share_Coefficient*master_Product_Summary[[6]][which(master_Product_Summary[[6]]$Category==filtered_final$Category[i] & master_Product_Summary[[6]]$Store_Group_Profit==filtered_final$Store_Group_Profit[i]),]$Unscaled_Shift_Profit
          filtered_final$Scaled_Beta_Profit[i]<-PCT_Share_Coefficient*master_Product_Summary[[6]][which(master_Product_Summary[[6]]$Category==filtered_final$Category[i] & master_Product_Summary[[6]]$Store_Group_Profit==filtered_final$Store_Group_Profit[i]),]$Unscaled_Beta_Profit
          filtered_final$Scaled_BP_Profit[i]<-PCT_Share_Coefficient*master_Product_Summary[[6]][which(master_Product_Summary[[6]]$Category==filtered_final$Category[i] & master_Product_Summary[[6]]$Store_Group_Profit==filtered_final$Store_Group_Profit[i]),]$Unscaled_Break_Point_Profit
          if(filtered_final$Exit_Flag[i]=="Exit"){
            filtered_final$Exit_Flag[i]<-"Exit"
          } else {
            filtered_final$Exit_Flag[i]<-0
          }
        }
        } else if(jobType=="Tiered") {
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

        if(methodology=="Traditional"){
          filtered_final$Exit_Flag[i]==0
        } else {
          filtered_final$Exit_Flag[i]<-"Exit"
        }
      }
    }


    filtered_final<-filtered_final[c("Store","Climate","VSG","Category","Store_Group_Sales","Store_Group_Profit","Store_Group_Units","Space","Sales","Scaled_Alpha_Sales","Scaled_Shift_Sales","Scaled_Beta_Sales","Scaled_BP_Sales","Units","Scaled_Alpha_Units","Scaled_Shift_Units","Scaled_Beta_Units","Scaled_BP_Units","Profit","Scaled_Alpha_Profit","Scaled_Shift_Profit","Scaled_Beta_Profit","Scaled_BP_Profit","PCT_Space_Change_Limit","Space.Lower.Limit","Space.Upper.Limit","PCT_Space_Lower_Limit","PCT_Space_Upper_Limit","Exit_Flag","Future_Space","Entry_Space","Climate_Group","Productivity_Group_Sales","Productivity_Group_Profit","Productivity_Group_Units","BOH_Dollar","BOH_Units","CC_Count_w_BOH","Profit_Percent","Receipts_Dollar","Receipts_Units")]
    #merging filtered data with the final data
    if(nrow(big_master_data)!=0){
      final_master<-rbind(final_master,filtered_final)
    } else {
      final_master<-filtered_final
      master_Product_Summary<-NULL
    }
  }
  #Final lower and upper limits variable generation
  if(jobType=="Tiered"){
    final_master$PCT_Change_Lower_Limit<-floor(final_master$Space*(1-final_master$PCT_Space_Change_Limit)/Increment_Size)*Increment_Size
    final_master$PCT_Change_Upper_Limit<-ceiling(final_master$Space*(1+final_master$PCT_Space_Change_Limit)/Increment_Size)*Increment_Size
  } else if(jobType=="Drill_Down" || is.null(PCT_Space_Change_Limit)==TRUE) {
    final_master$PCT_Change_Lower_Limit<-0
    final_master$PCT_Change_Upper_Limit<-0
  }
  final_master$PCT_of_Space_Lower_Limit<-floor(final_master$PCT_Space_Lower_Limit*(final_master$Future_Space-final_master$Entry_Space)/Increment_Size)*Increment_Size
  final_master$PCT_of_Space_Upper_Limit<-ceiling(final_master$PCT_Space_Upper_Limit*(final_master$Future_Space-final_master$Entry_Space)/Increment_Size)*Increment_Size


  for(h in (1:nrow(final_master))){
    final_master$Lower_Limit[h]<-ifelse(final_master$Exit_Flag[h]=="Exit",0,max(final_master$Space.Lower.Limit[h],final_master$PCT_Change_Lower_Limit[h],final_master$PCT_of_Space_Lower_Limit[h]))
    upper_limit<-c(final_master$Space.Upper.Limit[h],final_master$PCT_Change_Upper_Limit[h],final_master$PCT_of_Space_Upper_Limit[h])
    final_master$Upper_Limit[h]<-ifelse(final_master$Exit_Flag[h]=="Exit",0,min(upper_limit[which(upper_limit!=0)]))
  }


  final_master$Space_to_Fill<-final_master$Future_Space-final_master$Entry_Space
  final_master<-final_master[c("Store","Climate","VSG","Category","Store_Group_Sales","Store_Group_Profit","Store_Group_Units","Space","Sales","Scaled_Alpha_Sales","Scaled_Shift_Sales","Scaled_Beta_Sales","Scaled_BP_Sales","Units","Scaled_Alpha_Units","Scaled_Shift_Units","Scaled_Beta_Units","Scaled_BP_Units","Profit","Scaled_Alpha_Profit","Scaled_Shift_Profit","Scaled_Beta_Profit","Scaled_BP_Profit","Lower_Limit","Upper_Limit","Space_to_Fill","BOH_Dollar","BOH_Units","CC_Count_w_BOH","Profit_Percent","Receipts_Dollar","Receipts_Units")]

  #Sales penetration threshold check
  for(i in 1:nrow(final_master)){
    final_master$Lower_Limit[i]<-ifelse(final_master$Sales[which(final_master$Store[i] == final_master$Store & final_master$Category[i] == final_master$Category)]/sum(final_master$Sales[which(final_master$Store[i] == final_master$Store)])<sales_penetration_threshold,0,final_master$Lower_Limit[i])
    final_master$Upper_Limit[i]<-ifelse(final_master$Sales[which(final_master$Store[i] == final_master$Store & final_master$Category[i] == final_master$Category)]/sum(final_master$Sales[which(final_master$Store[i] == final_master$Store)])<sales_penetration_threshold,0,final_master$Upper_Limit[i])
  }
  final_out<-list(master_Product_Summary[[5]],master_Product_Summary[[6]],master_Product_Summary[[7]],final_master)

  return(final_out)
}
#
#Function calling
#output<-curvefitting_boundsetting(big_master_data,bound_input,Increment_Size,sales_weight,profit_weight,units_weight,PCT_Space_Change_Limit,jobType,methodology)
#Final output
#write.xlsx(output[1][[1]], "Analytics_Reference_Data.xlsx", sheetName="Sales",
#           col.names=TRUE, row.names=FALSE, append=FALSE)
#write.xlsx(output[2][[1]], "Analytics_Reference_Data.xlsx", sheetName="Profit",
#           col.names=TRUE, row.names=FALSE, append=TRUE)
#write.xlsx(output[3][[1]], "Analytics_Reference_Data.xlsx", sheetName="Units",
#           col.names=TRUE, row.names=FALSE, append=TRUE)

#write.csv(output[1], "Analytics_Reference_Data.csv",row.names=FALSE)
#write.csv(output[4], "Output_Data.csv",row.names=FALSE)
