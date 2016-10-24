#  rm(list=ls())
#
#  #Set working directory here
#  setwd("C:/Users/saurabh.a.trivedi/Desktop/Protyping Code/Misses Data Testing/")
#
#  #Run library for erf() function call
#  library(pracma)
#
#  #Import Raw data file
#  dataimprt<-read.csv("Output_Data.csv",header=TRUE,sep=",")
#
#  #Set increment size
#  incsize=0.5
#  #parameters for metric weights
#  Sales_weight<-0.4
#  Profit_weight<-0.3
#  Units_weight<-0.3
#
  Store_Optimization<-function(dataimprt,incsize,Sales_weight,Profit_weight,Units_weight){
  dataimprt$Trgt_BA_Space_Less_Brnd_Entry<-dataimprt$Space_to_Fill

  #Create important variables
  dataimprt$Space_Inc<-dataimprt$Space/incsize
  dataimprt$LBInc<-dataimprt$Lower_Limit/incsize
  dataimprt$UBInc<-dataimprt$Upper_Limit/incsize
  
  #Select data for any specific store through loop or integer
  for(j in (1:length(unique(dataimprt$Store)))){
    data<-dataimprt[which(dataimprt$Store %in% unique(dataimprt$Store)[j]),]
    
    #Increment Lower bound constraint
    data$Optimized_Space<-data$Space_Inc
    data$Optimized_Space<-as.integer(data$Optimized_Space)
    
    #Optimized Sales with starting values
    #Prepring lower bound and upper bound data
    for(s in (1:nrow(data))){
      data$LBInc[s]<-  round(data$LBInc[s],0)
      data$UBInc[s]<-  round(data$UBInc[s],0)
    }
    #Assigning space where historical space is lower than lower bound 
    data$boundbalance<-ifelse(data$Optimized_Space<data$LBInc,data$LBInc-data$Optimized_Space,ifelse(data$Optimized_Space>data$UBInc,data$UBInc-data$Optimized_Space,0))
    if(any(data$boundbalance!=0) && sum(data$boundbalance)==0){
      data$Optimized_Space<-data$Optimized_Space+data$boundbalance
      data$boundbalance<-0
    }
    if(any(data$boundbalance!=0) && sum(data$boundbalance)!=0){
      A=sum(data$boundbalance[which(data$boundbalance>0)])
      B=sum(data$boundbalance[which(data$boundbalance<0)])
      data$Optimized_Space<-data$Optimized_Space+data$boundbalance
      data$boundbalance<-0
      data$Optimized_VPE<-Sales_weight*(ifelse(data$Optimized_Space*incsize<data$Scaled_BP_Sales,data$Optimized_Space*incsize*data$Scaled_Alpha_Sales*(erf((data$Scaled_BP_Sales-data$Scaled_Shift_Sales)/(sqrt(2)*data$Scaled_Beta_Sales)))/data$Scaled_BP_Sales,data$Scaled_Alpha_Sales*erf((data$Optimized_Space*incsize-data$Scaled_Shift_Sales)/(sqrt(2)*data$Scaled_Beta_Sales)))) +
        Profit_weight*(ifelse(data$Optimized_Space*incsize<data$Scaled_BP_Profit,data$Optimized_Space*incsize*data$Scaled_Alpha_Profit*(erf((data$Scaled_BP_Profit-data$Scaled_Shift_Profit)/(sqrt(2)*data$Scaled_Beta_Profit)))/data$Scaled_BP_Profit,data$Scaled_Alpha_Profit*erf((data$Optimized_Space*incsize-data$Scaled_Shift_Profit)/(sqrt(2)*data$Scaled_Beta_Profit))))+
        Units_weight*(ifelse(data$Optimized_Space*incsize<data$Scaled_BP_Units,data$Optimized_Space*incsize*data$Scaled_Alpha_Units*(erf((data$Scaled_BP_Units-data$Scaled_Shift_Units)/(sqrt(2)*data$Scaled_Beta_Units)))/data$Scaled_BP_Units,data$Scaled_Alpha_Units*erf((data$Optimized_Space*incsize-data$Scaled_Shift_Units)/(sqrt(2)*data$Scaled_Beta_Units))))
      if(A-abs(B)<0){
        inc=1 
      } else {
        inc=-1
      }
      for(i in (1:abs(A-abs(B)))){
        
        #Isolating incremented sales from new sales
        incrVPE<-Sales_weight*(ifelse((data$Optimized_Space+inc)*incsize<data$Scaled_BP_Sales,(data$Optimized_Space+inc)*incsize*data$Scaled_Alpha_Sales*(erf((data$Scaled_BP_Sales-data$Scaled_Shift_Sales)/(sqrt(2)*data$Scaled_Beta_Sales)))/data$Scaled_BP_Sales,data$Scaled_Alpha_Sales*erf(((data$Optimized_Space+inc)*incsize-data$Scaled_Shift_Sales)/(sqrt(2)*data$Scaled_Beta_Sales)))) +
          Profit_weight*(ifelse((data$Optimized_Space+inc)*incsize<data$Scaled_BP_Profit,(data$Optimized_Space+inc)*incsize*data$Scaled_Alpha_Profit*(erf((data$Scaled_BP_Profit-data$Scaled_Shift_Profit)/(sqrt(2)*data$Scaled_Beta_Profit)))/data$Scaled_BP_Profit,data$Scaled_Alpha_Profit*erf(((data$Optimized_Space+inc)*incsize-data$Scaled_Shift_Profit)/(sqrt(2)*data$Scaled_Beta_Profit))))+
          Units_weight*(ifelse((data$Optimized_Space+inc)*incsize<data$Scaled_BP_Units,(data$Optimized_Space+inc)*incsize*data$Scaled_Alpha_Units*(erf((data$Scaled_BP_Units-data$Scaled_Shift_Units)/(sqrt(2)*data$Scaled_Beta_Units)))/data$Scaled_BP_Units,data$Scaled_Alpha_Units*erf(((data$Optimized_Space+inc)*incsize-data$Scaled_Shift_Units)/(sqrt(2)*data$Scaled_Beta_Units))))-data$Optimized_VPE
        
        #Constraint of Incremented Optimized Space should be less than Upper bound on Incremented space
        if(inc==-1){
          incrVPE<-ifelse((data$Optimized_Space+inc)<data$LBInc,0,incrVPE)
        } else { 
          
          incrVPE<-ifelse((data$Optimized_Space+inc)>data$UBInc,0,incrVPE)
        }
        
        #Adding increment of space againt correct product where maximizing sales
        incrSpace=ifelse(max(incrVPE[which(incrVPE!=0)])==incrVPE,inc,0)
        data$Optimized_Space<-data$Optimized_Space+incrSpace
        
        #New Sales with new space
        data$Optimized_VPE<-Sales_weight*(ifelse(data$Optimized_Space*incsize<data$Scaled_BP_Sales,data$Optimized_Space*incsize*data$Scaled_Alpha_Sales*(erf((data$Scaled_BP_Sales-data$Scaled_Shift_Sales)/(sqrt(2)*data$Scaled_Beta_Sales)))/data$Scaled_BP_Sales,data$Scaled_Alpha_Sales*erf((data$Optimized_Space*incsize-data$Scaled_Shift_Sales)/(sqrt(2)*data$Scaled_Beta_Sales)))) +
          Profit_weight*(ifelse(data$Optimized_Space*incsize<data$Scaled_BP_Profit,data$Optimized_Space*incsize*data$Scaled_Alpha_Profit*(erf((data$Scaled_BP_Profit-data$Scaled_Shift_Profit)/(sqrt(2)*data$Scaled_Beta_Profit)))/data$Scaled_BP_Profit,data$Scaled_Alpha_Profit*erf((data$Optimized_Space*incsize-data$Scaled_Shift_Profit)/(sqrt(2)*data$Scaled_Beta_Profit))))+
          Units_weight*(ifelse(data$Optimized_Space*incsize<data$Scaled_BP_Units,data$Optimized_Space*incsize*data$Scaled_Alpha_Units*(erf((data$Scaled_BP_Units-data$Scaled_Shift_Units)/(sqrt(2)*data$Scaled_Beta_Units)))/data$Scaled_BP_Units,data$Scaled_Alpha_Units*erf((data$Optimized_Space*incsize-data$Scaled_Shift_Units)/(sqrt(2)*data$Scaled_Beta_Units))))
        ################################################################################  
        
      }
    }
    #Total sales at historical space of the stores
    data$Optimized_VPE<-Sales_weight*(ifelse(data$Optimized_Space*incsize<data$Scaled_BP_Sales,data$Optimized_Space*incsize*data$Scaled_Alpha_Sales*(erf((data$Scaled_BP_Sales-data$Scaled_Shift_Sales)/(sqrt(2)*data$Scaled_Beta_Sales)))/data$Scaled_BP_Sales,data$Scaled_Alpha_Sales*erf((data$Optimized_Space*incsize-data$Scaled_Shift_Sales)/(sqrt(2)*data$Scaled_Beta_Sales)))) +
      Profit_weight*(ifelse(data$Optimized_Space*incsize<data$Scaled_BP_Profit,data$Optimized_Space*incsize*data$Scaled_Alpha_Profit*(erf((data$Scaled_BP_Profit-data$Scaled_Shift_Profit)/(sqrt(2)*data$Scaled_Beta_Profit)))/data$Scaled_BP_Profit,data$Scaled_Alpha_Profit*erf((data$Optimized_Space*incsize-data$Scaled_Shift_Profit)/(sqrt(2)*data$Scaled_Beta_Profit))))+
      Units_weight*(ifelse(data$Optimized_Space*incsize<data$Scaled_BP_Units,data$Optimized_Space*incsize*data$Scaled_Alpha_Units*(erf((data$Scaled_BP_Units-data$Scaled_Shift_Units)/(sqrt(2)*data$Scaled_Beta_Units)))/data$Scaled_BP_Units,data$Scaled_Alpha_Units*erf((data$Optimized_Space*incsize-data$Scaled_Shift_Units)/(sqrt(2)*data$Scaled_Beta_Units))))
    VPEi<-sum(data$Optimized_VPE)
    #Loop for total iterations to be run
    for(i in (1:75)){
      #Randomly select anyone product to adjust -1 from the space
      index<-round(runif(1,1,nrow(data)),0)
      #Test whether space for the selected product is greater than lower bound
      if(data$Optimized_Space[index]==data$LBInc[index]){
        
      } else {
        #t=1
        #Loop to test whether increase in space by 1 unit on anyone product will increase sales from the last observed sales
        for(t in (1:nrow(data))){
          #Test whether product is not the same product from where -1 is done or test whether product space is not equal to upper bound
          if(t==index||data$Optimized_Space[t]==data$UBInc[t]){
            
          } else{
            #Calculation of -1 or +1 from the product
            data$Optimized_Space[index]<-data$Optimized_Space[index]-1  
            data$Optimized_Space[t]<-data$Optimized_Space[t]+1
            data$Optimized_VPE<-Sales_weight*(ifelse(data$Optimized_Space*incsize<data$Scaled_BP_Sales,data$Optimized_Space*incsize*data$Scaled_Alpha_Sales*(erf((data$Scaled_BP_Sales-data$Scaled_Shift_Sales)/(sqrt(2)*data$Scaled_Beta_Sales)))/data$Scaled_BP_Sales,data$Scaled_Alpha_Sales*erf((data$Optimized_Space*incsize-data$Scaled_Shift_Sales)/(sqrt(2)*data$Scaled_Beta_Sales)))) +
              Profit_weight*(ifelse(data$Optimized_Space*incsize<data$Scaled_BP_Profit,data$Optimized_Space*incsize*data$Scaled_Alpha_Profit*(erf((data$Scaled_BP_Profit-data$Scaled_Shift_Profit)/(sqrt(2)*data$Scaled_Beta_Profit)))/data$Scaled_BP_Profit,data$Scaled_Alpha_Profit*erf((data$Optimized_Space*incsize-data$Scaled_Shift_Profit)/(sqrt(2)*data$Scaled_Beta_Profit))))+
              Units_weight*(ifelse(data$Optimized_Space*incsize<data$Scaled_BP_Units,data$Optimized_Space*incsize*data$Scaled_Alpha_Units*(erf((data$Scaled_BP_Units-data$Scaled_Shift_Units)/(sqrt(2)*data$Scaled_Beta_Units)))/data$Scaled_BP_Units,data$Scaled_Alpha_Units*erf((data$Optimized_Space*incsize-data$Scaled_Shift_Units)/(sqrt(2)*data$Scaled_Beta_Units))))
            #Conditional check whether there is an increase in sales from the last observed sales  
            if(sum(data$Optimized_VPE)>VPEi){
              i=0
              VPEi=sum(data$Optimized_VPE)
              break
            } else{
              #If current sales is not greater than last observed sales then reset the changes    
              data$Optimized_Space[index]<-data$Optimized_Space[index]+1
              data$Optimized_Space[t]<-data$Optimized_Space[t]-1
            }                       
          } 
        }
      }
    }
    #Constraint of Incremented Optimized Space should be less than Upper bound on Incremented space
    #New Sales with new space
    limit<-round(sum(data$Space_Inc),0)-sum(data$Optimized_Space)
    
    #Loop for total number of iterations to be run after accounting for Lower Bound Space allocation
    #i=1
    if(limit>0){
      for(i in (1:limit)){
        inc=1
        #Isolating incremented sales from new sales
        incrVPE<-Sales_weight*(ifelse((data$Optimized_Space+inc)*incsize<data$Scaled_BP_Sales,(data$Optimized_Space+inc)*incsize*data$Scaled_Alpha_Sales*(erf((data$Scaled_BP_Sales-data$Scaled_Shift_Sales)/(sqrt(2)*data$Scaled_Beta_Sales)))/data$Scaled_BP_Sales,data$Scaled_Alpha_Sales*erf(((data$Optimized_Space+inc)*incsize-data$Scaled_Shift_Sales)/(sqrt(2)*data$Scaled_Beta_Sales)))) +
          Profit_weight*(ifelse((data$Optimized_Space+inc)*incsize<data$Scaled_BP_Profit,(data$Optimized_Space+inc)*incsize*data$Scaled_Alpha_Profit*(erf((data$Scaled_BP_Profit-data$Scaled_Shift_Profit)/(sqrt(2)*data$Scaled_Beta_Profit)))/data$Scaled_BP_Profit,data$Scaled_Alpha_Profit*erf(((data$Optimized_Space+inc)*incsize-data$Scaled_Shift_Profit)/(sqrt(2)*data$Scaled_Beta_Profit))))+
          Units_weight*(ifelse((data$Optimized_Space+inc)*incsize<data$Scaled_BP_Units,(data$Optimized_Space+inc)*incsize*data$Scaled_Alpha_Units*(erf((data$Scaled_BP_Units-data$Scaled_Shift_Units)/(sqrt(2)*data$Scaled_Beta_Units)))/data$Scaled_BP_Units,data$Scaled_Alpha_Units*erf(((data$Optimized_Space+inc)*incsize-data$Scaled_Shift_Units)/(sqrt(2)*data$Scaled_Beta_Units))))-data$Optimized_VPE
        
        #Constraint of Incremented Optimized Space should be less than Upper bound on Incremented space
        incrVPE<-ifelse((data$Optimized_Space+inc)>data$UBInc,0,incrVPE)
        
        #Adding increment of space againt correct product where maximizing sales
        incrSpace=ifelse(max(incrVPE[which(incrVPE!=0)])==incrVPE,inc,0)
        data$Optimized_Space<-data$Optimized_Space+incrSpace
        
        #New Sales with new space
        data$Optimized_VPE<-Sales_weight*(ifelse(data$Optimized_Space*incsize<data$Scaled_BP_Sales,data$Optimized_Space*incsize*data$Scaled_Alpha_Sales*(erf((data$Scaled_BP_Sales-data$Scaled_Shift_Sales)/(sqrt(2)*data$Scaled_Beta_Sales)))/data$Scaled_BP_Sales,data$Scaled_Alpha_Sales*erf((data$Optimized_Space*incsize-data$Scaled_Shift_Sales)/(sqrt(2)*data$Scaled_Beta_Sales)))) +
          Profit_weight*(ifelse(data$Optimized_Space*incsize<data$Scaled_BP_Profit,data$Optimized_Space*incsize*data$Scaled_Alpha_Profit*(erf((data$Scaled_BP_Profit-data$Scaled_Shift_Profit)/(sqrt(2)*data$Scaled_Beta_Profit)))/data$Scaled_BP_Profit,data$Scaled_Alpha_Profit*erf((data$Optimized_Space*incsize-data$Scaled_Shift_Profit)/(sqrt(2)*data$Scaled_Beta_Profit))))+
          Units_weight*(ifelse(data$Optimized_Space*incsize<data$Scaled_BP_Units,data$Optimized_Space*incsize*data$Scaled_Alpha_Units*(erf((data$Scaled_BP_Units-data$Scaled_Shift_Units)/(sqrt(2)*data$Scaled_Beta_Units)))/data$Scaled_BP_Units,data$Scaled_Alpha_Units*erf((data$Optimized_Space*incsize-data$Scaled_Shift_Units)/(sqrt(2)*data$Scaled_Beta_Units))))
      }
    }
    if(data$Trgt_BA_Space_Less_Brnd_Entry[1]/incsize!=round(sum(data$Optimized_Space),0)){
      A=round(sum(data$Optimized_Space),0)
      B=data$Trgt_BA_Space_Less_Brnd_Entry[1]/incsize
      if(A-abs(B)<0){
        inc=1 
      } else {
        inc=-1
      }
      for(i in (1:abs(A-abs(B)))){
        
        #Isolating incremented sales from new sales
        incrVPE<-Sales_weight*(ifelse((data$Optimized_Space+inc)*incsize<data$Scaled_BP_Sales,(data$Optimized_Space+inc)*incsize*data$Scaled_Alpha_Sales*(erf((data$Scaled_BP_Sales-data$Scaled_Shift_Sales)/(sqrt(2)*data$Scaled_Beta_Sales)))/data$Scaled_BP_Sales,data$Scaled_Alpha_Sales*erf(((data$Optimized_Space+inc)*incsize-data$Scaled_Shift_Sales)/(sqrt(2)*data$Scaled_Beta_Sales)))) +
          Profit_weight*(ifelse((data$Optimized_Space+inc)*incsize<data$Scaled_BP_Profit,(data$Optimized_Space+inc)*incsize*data$Scaled_Alpha_Profit*(erf((data$Scaled_BP_Profit-data$Scaled_Shift_Profit)/(sqrt(2)*data$Scaled_Beta_Profit)))/data$Scaled_BP_Profit,data$Scaled_Alpha_Profit*erf(((data$Optimized_Space+inc)*incsize-data$Scaled_Shift_Profit)/(sqrt(2)*data$Scaled_Beta_Profit))))+
          Units_weight*(ifelse((data$Optimized_Space+inc)*incsize<data$Scaled_BP_Units,(data$Optimized_Space+inc)*incsize*data$Scaled_Alpha_Units*(erf((data$Scaled_BP_Units-data$Scaled_Shift_Units)/(sqrt(2)*data$Scaled_Beta_Units)))/data$Scaled_BP_Units,data$Scaled_Alpha_Units*erf(((data$Optimized_Space+inc)*incsize-data$Scaled_Shift_Units)/(sqrt(2)*data$Scaled_Beta_Units))))-data$Optimized_VPE
        
        #Constraint of Incremented Optimized Space should be less than Upper bound on Incremented space
        if(inc==-1){
          incrVPE<-ifelse((data$Optimized_Space+inc)<data$LBInc,0,incrVPE)
        } else { 
          
          incrVPE<-ifelse((data$Optimized_Space+inc)>data$UBInc,0,incrVPE)
        }
        
        #Adding increment of space againt correct product where maximizing sales
        incrSpace=ifelse(max(incrVPE[which(incrVPE!=0)])==incrVPE,inc,0)
        data$Optimized_Space<-data$Optimized_Space+incrSpace
        
        #New Sales with new space
        data$Optimized_VPE<-Sales_weight*(ifelse(data$Optimized_Space*incsize<data$Scaled_BP_Sales,data$Optimized_Space*incsize*data$Scaled_Alpha_Sales*(erf((data$Scaled_BP_Sales-data$Scaled_Shift_Sales)/(sqrt(2)*data$Scaled_Beta_Sales)))/data$Scaled_BP_Sales,data$Scaled_Alpha_Sales*erf((data$Optimized_Space*incsize-data$Scaled_Shift_Sales)/(sqrt(2)*data$Scaled_Beta_Sales)))) +
          Profit_weight*(ifelse(data$Optimized_Space*incsize<data$Scaled_BP_Profit,data$Optimized_Space*incsize*data$Scaled_Alpha_Profit*(erf((data$Scaled_BP_Profit-data$Scaled_Shift_Profit)/(sqrt(2)*data$Scaled_Beta_Profit)))/data$Scaled_BP_Profit,data$Scaled_Alpha_Profit*erf((data$Optimized_Space*incsize-data$Scaled_Shift_Profit)/(sqrt(2)*data$Scaled_Beta_Profit))))+
          Units_weight*(ifelse(data$Optimized_Space*incsize<data$Scaled_BP_Units,data$Optimized_Space*incsize*data$Scaled_Alpha_Units*(erf((data$Scaled_BP_Units-data$Scaled_Shift_Units)/(sqrt(2)*data$Scaled_Beta_Units)))/data$Scaled_BP_Units,data$Scaled_Alpha_Units*erf((data$Optimized_Space*incsize-data$Scaled_Shift_Units)/(sqrt(2)*data$Scaled_Beta_Units))))

      }
    }
    if(j==1){
      final<-data
    } else {
      final<-rbind(final,data)
    }
    
  }
  
  final$Optimized_Space<-final$Optimized_Space*incsize
  final<-final[c("Store","Climate","VSG","Category","Store_Group","Space","Sales","Scaled_Alpha_Sales","Scaled_Shift_Sales","Scaled_Beta_Sales","Scaled_BP_Sales","Units","Scaled_Alpha_Units","Scaled_Shift_Units","Scaled_Beta_Units","Scaled_BP_Units","Profit","Scaled_Alpha_Profit","Scaled_Shift_Profit","Scaled_Beta_Profit","Scaled_BP_Profit","Lower_Limit","Upper_Limit","Optimized_Space")]
  return(final)
  }
  
#  output<-Store_Optimization(dataimprt,incsize,Sales_weight,Profit_weight,Units_weight)
#  write.csv(output,"Optimization_Result.csv",row.names=FALSE)
