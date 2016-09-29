# Accenture & Kohl's Proprietary and Confidential Information.
# This code contains Accenture Materials; the code and the concepts used in it are subject to the 
# Top Down Macro Space Optimization Approach agreement between Accenture & Kohl's.  

# rm(list=ls())  #Clear historical data from R server

# Curve Fitting and Bound Setting Function
curvefitting_boundsetting<-function(master,bound_input,increment,pct_chg_limit,sls_pen_thresh,jobType,optimizationType){
  
  # BEGIN curve-fitting
  library(pracma) #For error function
  library(tidyr)  #For data manipulation
  library(nloptr) #For running optimization to find unscaled coefficients
  library(gdata)
#  library(xlsx)

  #	Minimal Store-Category History Filters
  space_filter <- 0.1
  Sales_filter <- 20
  Profit_filter <- 5
  Units_filter <- 5
  
  #	Minimal Category-Climate Group History Filters
  strcount_filter <- 100
  avgSales_filter <- 200
  avgProfit_filter <- 50
  avgUnits_filter <- 50

  # Rename column titles where column title contains blanks
  names(master)[c(12,13,14,15,16,17)] <- c("BOH_Dollar","BOH_Units","CC_Count_w_BOH","Profit_Percent","Receipts_Dollar","Receipts_Units")

  # Assign Climate Groups
  master$Climate_Group <- as.character(master$Climate)
  if(jobType == "Tiered"){
    master$Climate_Group <- "NA"
  } else {
    master$Climate_Group <- ifelse((master$Climate_Group == "HOT" | master$Climate_Group == "SUPER HOT"), "HOT & SH", master$Climate_Group)
  }
  master$Category_CG <- paste0(master$Category,"-",master$Climate_Group)
  
  # Initialize data frame to hold unscaled curve information (analytics reference data)
  ref <- structure(list(Category_SG=character()), class = "data.frame")

  k <- 0
  firstmetric <- TRUE
  
  for(target in c("Sales","Profit","Units")){

    # Business Area Productivity Calculation
    master[,paste0("BA_Prod_",target)] <- NA
    for(j in 1:nrow(master)){
      master[,paste0("BA_Prod_",target)][j] <- sum(master[,target][which(master$Store %in% master$Store[j])])/sum(master$Space[which(master$Store %in% master$Store[j])])
    }

    #	Flag Store-Categories with Minimal History
    master[,paste0("Str_Cat_Flag_",target)] <- 0
    master[,paste0("Str_Cat_Flag_",target)] <- ifelse((master$Space < space_filter | master[,target] < get(paste0(target,"_filter"))), 1, 0)

    #	Flag Category-Climate Groups with Minimal History
    eligiblePrelim<-master[which(master[,paste0("Str_Cat_Flag_",target)] == 0),]
    targetStats = do.call(data.frame, aggregate(x = eligiblePrelim[target], by = eligiblePrelim["Category_CG"], FUN = function(x) c(Average = mean(x), Str_Count = length(x))))
    remove(eligiblePrelim)
    targetStats[,paste0("Cat_CG_Flag_",target)] <- 0
    targetStats[,paste0("Cat_CG_Flag_",target)] <- ifelse((targetStats[,paste0(target,".Str_Count")] < strcount_filter | targetStats[,paste0(target,".Average")] < get(paste0("avg",target,"_filter"))), 1, 0)
    master <- merge(master,targetStats[ , c("Category_CG", paste0("Cat_CG_Flag_",target))],by="Category_CG",all.x=TRUE)
    master[,paste0("Cat_CG_Flag_",target)][is.na(master[,paste0("Cat_CG_Flag_",target)])] <- 0

    # Select the data eligible for curve-fitting, which passed all filters
    eligible <- master[which(master[,paste0("Str_Cat_Flag_",target)] == 0 & master[,paste0("Cat_CG_Flag_",target)] == 0),]
    eligible <- eligible[c("Store","Category", "Category_CG","Climate_Group",paste0("BA_Prod_",target),"Space",target)]
    
    # Assign Productivity Groups
    prodStats <- do.call(data.frame, 
                         aggregate(x = eligible[,paste0("BA_Prod_",target)], 
                                   by = eligible["Category_CG"], 
                                   FUN = function(x) c(
                                     Str_Count = length(x), 
                                     q1 = quantile(x, probs = 0.25), 
                                     q2 = quantile(x, probs = 0.5), 
                                     q3 = quantile(x, probs = 0.75))))
    rownames(prodStats) <- prodStats$Category_CG
    eligible[,paste0("BA_Prod_Group_",target)] <- NA
    for(q in (1:nrow(eligible))){
      currCatCG <- eligible[q,"Category_CG"]
      if(prodStats[currCatCG,"x.Str_Count"] >= 600){
        eligible[q,paste0("BA_Prod_Group_",target)] <- 
          ifelse(eligible[q,paste0("BA_Prod_",target)]<prodStats[currCatCG,"x.q1.25."],
                 "Low",
                 ifelse(eligible[q,paste0("BA_Prod_",target)]>prodStats[currCatCG,"x.q3.75."],
                        "High",
                        "Medium"))
      } else if (prodStats[currCatCG,"x.Str_Count"] >= 300){
        eligible[q,paste0("BA_Prod_Group_",target)] <- 
          ifelse(eligible[q,paste0("BA_Prod_",target)]<prodStats[currCatCG,"x.q2.50."],
                 "Low",
                 "High")
      } else {
        eligible[q,paste0("BA_Prod_Group_",target)] <- "NA"
      }
    }

    # Assign Store Groups
    eligible[paste0("Store_Group_",target)] <- "NA"
    if(jobType == "Tiered"){
      eligible[paste0("Store_Group_",target)] <- eligible[,paste0("BA_Prod_Group_",target)]
    } else {
      eligible[paste0("Store_Group_",target)] <- paste0(eligible$Climate_Group,"-",eligible[,paste0("BA_Prod_Group_",target)])
    }
    eligible$Category_SG <- paste0(eligible$Category,"-",eligible[,paste0("Store_Group_",target)])

    # Loop through each category-store group to generate unscaled coefficients
    cat_sg_list <- unique(eligible$Category_SG)
    first_curve <- TRUE
    for (j in 1:nrow(data.frame(cat_sg_list))){
      
      k = k + 1
      
      cfdata <- eligible[which(eligible$Category_SG %in% cat_sg_list[j]),]
      
      x <- cat_sg_list[j]
      ref[k,"Category_SG"] <- x
      ref[k,"Category"] <- substr(x, 1, regexpr("-", x)-1)
      ref[k,"Store_Group"] <- substr(x, regexpr("-", x)+1,nchar(x))
      ref[k,"Metric"] <- target
      
      ref[k,"Correlation"] <- cor(cfdata[,c(target,"Space")])[1,2]
      
      # Handle special case of one unique space value
      if(length(unique(cfdata$Space)) == 1 ){
        
        ref[k,"Special Case"] <- "One Unique Space"
        ref[k,"Alpha_Seed"] <- mean(cfdata[,target])
        ref[k,"Shift_Seed"] <- 0
        spacebetaseed <- unique(cfdata$Space)/2
        targetbetaseed <- ref[k,"Alpha_Seed"]/2
        ref[k,"Beta_Seed"] <- -(spacebetaseed-ref[k,"Shift_Seed"])/qnorm((1-targetbetaseed/ref[k,"Alpha_Seed"])/2)
        Estimate<-c(ref[k,"Alpha_Seed"],ref[k,"Beta_Seed"],ref[k,"Shift_Seed"])
        coef<-data.frame(Estimate)
        coef<-data.frame(t(data.frame(coef)))
        colnames(coef)<-c("Alpha","Beta","Shift")

      } else {

        ref[k,"Special Case"] <- "NA"
        
        # Starting values for unscaled coefficients
        ref[k,"Alpha_Seed"] <- max(mean(cfdata[,target]),mean(cfdata[,target][which(cfdata$Space>=quantile(cfdata$Space, .75))]))
        spacebetaseed <- mean(cfdata$Space[which(cfdata$Space<=quantile(cfdata$Space, .25))])
        targetbetaseed <- min(ref[k,"Alpha_Seed"], mean(cfdata[,target][which(cfdata$Space<=quantile(cfdata$Space, .25))]))
        ref[k,"Shift_Seed"] <- 0
        ref[k,"Beta_Seed"] <- max(0,-(spacebetaseed-ref[k,"Shift_Seed"])/qnorm((1-targetbetaseed/ref[k,"Alpha_Seed"])/2))
        
        # Lower bounds for unscaled coefficients
        ref[k,"Alpha_LB"] <- 0
        ref[k,"Shift_LB"] <- 0
        spacebetalb <- quantile(cfdata$Space, 0.01)
        mean_target <- mean(cfdata[,target])
        targetbetalblinearprod <- spacebetalb * (sum(cfdata[,target])/sum(cfdata$Space))
        targetbetalb <- (mean_target + targetbetalblinearprod)/2
        ref[k,"Beta_LB"] <- -(spacebetalb-ref[k,"Shift_Seed"])/qnorm((1-targetbetalb/mean_target)/2)
        ref[k,"Beta_Seed"] <- max(ref[k,"Beta_Seed"],ref[k,"Beta_LB"])
        
        # Upper bounds for unscaled coefficients
        ref[k,"Alpha_UB"] <- quantile(cfdata[,target], 0.95)
        ref[k,"Beta_UB"] <- Inf
        ref[k,"Shift_UB"] <- ifelse(min(cfdata$Space) < 2 * max(1, increment) | ref[k,"Correlation"] <= 0.2, 0, min(cfdata$Space) - max(1, increment))
        
        # Define functional form
        Space <- cfdata$Space
        targetset <- cfdata[,target]
        predfun <- function(par) {
          Alpha <- par[1]
          Beta <- par[2]
          Shift <- par[3]
          rhat <- Alpha*erf((Space-Shift)/(sqrt(2)*Beta))
          r <- sum((targetset - rhat)^2)
          return(r)
        }

        # Store unscaled coefficient seed values and bounds
        x0 <- c(ref[k,"Alpha_Seed"],ref[k,"Beta_Seed"],ref[k,"Shift_Seed"])
        lower_bounds <- c(ref[k,"Alpha_LB"],ref[k,"Beta_LB"],ref[k,"Shift_LB"])
        upper_bounds <- c(ref[k,"Alpha_UB"],ref[k,"Beta_UB"],ref[k,"Shift_UB"])
        
        # Solve for unscaled coefficients using optimization function "auglag"
        model <- auglag(x0, predfun, gr=NULL, lower=lower_bounds, upper=upper_bounds, localsolver=c("MMA"))
        
        #Extract model results 
        coef <- t(data.frame(model$par))
        
      }
      
      # Finalize unscaled coefficients
      coef[,c(1,2,3)] <- round(coef[,c(1,2,3)],4)
      ref[k,"Unscaled_Alpha"] <- coef[1]
      ref[k,"Unscaled_Beta"] <- coef[2]
      ref[k,"Unscaled_Shift"] <- coef[3]
      ref[k,"Unscaled_BP"] <- ifelse(ref[k,"Unscaled_Shift"]==0,0,quantile(cfdata$Space, 0.01))
      ref[k,"Unscaled_BP_Target"] <- ref[k,"Unscaled_Alpha"]*erf((ref[k,"Unscaled_BP"]-ref[k,"Unscaled_Shift"])/(sqrt(2)*ref[k,"Unscaled_Beta"]))
      ref[k,"Unscaled_BP_Prod"] <- ref[k,"Unscaled_BP_Target"]/ref[k,"Unscaled_BP"]
      ref[k,"Critical_Point"] <- ref[k,"Unscaled_Shift"]+sqrt(2)*ref[k,"Unscaled_Beta"]*sqrt(log(sqrt(7/11)*(ref[k,"Unscaled_Alpha"]/ref[k,"Unscaled_Beta"])))
      
      # Calculate goodness-of-fit statistics
      Prediction <- ref[k,"Unscaled_Alpha"]*erf((cfdata$Space-ref[k,"Unscaled_Shift"])/(sqrt(2)*ref[k,"Unscaled_Beta"]))
      ref[k,"Quasi_R_Squared"] <- 1-sum((cfdata[,target]-Prediction)^2)/(length(cfdata[,target])*var(cfdata[,target]))  # Valid when reasonably close to linear
      ref[k,"MAPE"] <- mean(abs(Prediction-(cfdata[,target]))/(cfdata[,target])) # Mean Absolute Percentage Error

      # BEGIN store scaling calculations
      
      # Calculate the historical productivity of the store-category
      cfdata[,paste0("Productivity_",target)] <- cfdata[,target]/cfdata$Space
      
      # Calculate the expected target value based on the unscaled curve at the store-category's historical space
      cfdata[,paste0("Unscaled_Predicted_",target)] <- 
        ifelse(cfdata$Space<ref[k,"Unscaled_BP"],
               cfdata$Space*ref[k,"Unscaled_BP_Prod"],
               ref[k,"Unscaled_Alpha"]*erf((cfdata$Space-ref[k,"Unscaled_Shift"])/(sqrt(2)*ref[k,"Unscaled_Beta"])))
      
      # Determine the scaling jobType for use in scaling calculations
      cfdata[,paste0("Scaling_jobType_",target)] <- 
        ifelse(cfdata$Space>ref[k,"Critical_Point"],
               "E", # jobType E: space above where the curve has flattened out
               ifelse(cfdata[,target]>=cfdata[,paste0("Unscaled_Predicted_",target)],
                      ifelse(cfdata[,target]>=ref[k,"Unscaled_BP_Target"],
                             "A", # jobType A: over-performs unscaled curve and is outside unscaled BP/BP Target rectangle 
                             "C"), # jobType C: over-performs unscaled curve and is inside unscaled BP/BP Target rectangle
                      ifelse(cfdata$Space>=ref[k,"Unscaled_BP"],
                             "B", # jobType B: under-performs unscaled curve and is outside unscaled BP/BP Target rectangle
                             "D"))) # jobType D: under-performs unscaled curve and is inside unscaled BP/BP Target rectangle
      
      # Find the space of the data point that the scaled S curve should go through
      cfdata[,paste0("Space2Solve4_",target)] <- 
        ifelse((cfdata[,paste0("Scaling_jobType_",target)]=="A" | cfdata[,paste0("Scaling_jobType_",target)]=="B"),
               cfdata$Space,
               ref[k,"Unscaled_BP"])
      
      # Find the target of the data point that the scaled S curve should go through
      cfdata[,paste0(target,"2Solve4")] <- 
        ifelse((cfdata[,paste0("Scaling_jobType_",target)]=="A" | cfdata[,paste0("Scaling_jobType_",target)]=="B"),
               cfdata[,target],
               ref[k,"Unscaled_BP"]*cfdata[,paste0("Productivity_",target)])
      
      # Scale the alpha by the difference between store's historical target value and the unscaled expected target value
      cfdata[,paste0("Scaled_Alpha_",target)] <- 
        ref[k,"Unscaled_Alpha"]+cfdata[,target]-cfdata[,paste0("Unscaled_Predicted_",target)]
      
      # Scale the shift, keeping slope steady relative to unscaled curve unless/until break point assumption is not needed
      cfdata[,paste0("Scaled_Shift_",target)] <- 
        ifelse((cfdata[,paste0("Scaling_jobType_",target)]=="A" | cfdata[,paste0("Scaling_jobType_",target)]=="C"),
               pmax(0,cfdata[,paste0("Space2Solve4_",target)]+ref[k,"Unscaled_Beta"]*qnorm((1-(cfdata[,paste0(target,"2Solve4")]/cfdata[,paste0("Scaled_Alpha_",target)]))/2)),
               ref[k,"Unscaled_Shift"])

      # Scale the beta such that the final scaled S curve goes through the identified point
      cfdata[,paste0("Scaled_Beta_",target)] <- 
        ifelse(cfdata[,paste0("Scaling_jobType_",target)]=="E",
               ref[k,"Unscaled_Beta"],
               (cfdata[,paste0("Scaled_Shift_",target)]-cfdata[,paste0("Space2Solve4_",target)])/(qnorm((1-(cfdata[,paste0(target,"2Solve4")]/cfdata[,paste0("Scaled_Alpha_",target)]))/2)))
      
      # Scale the break point when a linear assumption is needed
      if(ref[k,"Unscaled_BP"]==0){
       
        cfdata[,paste0("Scaled_BP_",target)] <- 0
        cfdata[,paste0(target,"_at_Scaled_BP")] <- 0
        cfdata[,paste0("Prod_at_Scaled_BP_",target)] <- 0
         
      } else{
        
        cfdata[,paste0("Scaled_BP_",target)] <- 
          ifelse(!cfdata[,paste0("Scaling_jobType_",target)]=="A",
                 ref[k,"Unscaled_BP"],
                 cfdata[,paste0("Scaled_Shift_",target)]-cfdata[,paste0("Scaled_Beta_",target)]*qnorm((1-(ref[k,"Unscaled_BP_Target"]/cfdata[,paste0("Scaled_Alpha_",target)]))/2))
        
        cfdata[,paste0(target,"_at_Scaled_BP")] <- cfdata[,paste0("Scaled_Alpha_",target)]*erf((cfdata[,paste0("Scaled_BP_",target)]-cfdata[,paste0("Scaled_Shift_",target)])/(sqrt(2)*cfdata[,paste0("Scaled_Beta_",target)]))
        
        cfdata[,paste0("Prod_at_Scaled_BP_",target)] <- cfdata[,paste0(target,"_at_Scaled_BP")]/cfdata[,paste0("Scaled_BP_",target)]
      
      }
      
      # Calculate the expected target value based on the scaled curve at historical space, which equals historical target
      cfdata[,paste0("Scaled_Predicted_",target)] <- 
        ifelse(cfdata$Space<cfdata[,paste0("Scaled_BP_",target)],
               cfdata$Space*cfdata[,paste0("Prod_at_Scaled_BP_",target)],
               cfdata[,paste0("Scaled_Alpha_",target)]*erf((cfdata$Space-cfdata[,paste0("Scaled_Shift_",target)])/(sqrt(2)*cfdata[,paste0("Scaled_Beta_",target)])))
      
      # END store scaling calculations
      
      drop <- c("Category_SG",paste0("Productivity_",target),paste0("Unscaled_Predicted_",target),paste0("Scaling_jobType_",target),
                paste0("Space2Solve4_",target),paste0(target,"2Solve4"),paste0(target,"_at_Scaled_BP"),
                paste0("Prod_at_Scaled_BP_",target),paste0("Scaled_Predicted_",target))
      cfdata = cfdata[,!(names(cfdata) %in% drop)]

      # Combine coefficient information for all curves within the same metric
      if(first_curve){
        eligibleScaled <- cfdata
        first_curve <- FALSE
      } else{
        eligibleScaled <- rbind(eligibleScaled,cfdata)
      }
      
    }
    
    # Merge coefficient information for each metric into the master data set
    master <- merge(master, eligibleScaled, by=c("Store","Category","Category_CG","Climate_Group",paste0("BA_Prod_",target),"Space",target), all.x=TRUE)
    
    # Use 1's and 0's for any data points that were not part of curve-fitting
    master[,paste0("Scaled_Alpha_",target)][is.na(master[,paste0("Scaled_Alpha_",target)])] <- 0
    master[,paste0("Scaled_Shift_",target)][is.na(master[,paste0("Scaled_Shift_",target)])] <- 0
    master[,paste0("Scaled_Beta_",target)][is.na(master[,paste0("Scaled_Beta_",target)])] <- 1
    master[,paste0("Scaled_BP_",target)][is.na(master[,paste0("Scaled_BP_",target)])] <- 0
    
    # For drill downs, use assumption for store-category history filtered stores
    # TODO: Write code
    
  }
  
  # END curve-fitting
  
  # BEGIN bound-setting

  # Add name to category bound table and merge with master data set
  names(bound_input)[1] <- "Category"
  master <- merge(master, bound_input, by="Category",all.x=TRUE)
  
  # Calculate space to fill in the optimization
  master$Space_to_Fill <- master$Future_Space-master$Entry_Space
  
  # Apply category percent of space bounds to store-category level
  master$PCT_of_Space_Lower_Limit <- floor(master$PCT_Space_Lower_Limit*master$Space_to_Fill/increment)*increment
  master$PCT_of_Space_Upper_Limit <- ceiling(master$PCT_Space_Upper_Limit*master$Space_to_Fill/increment)*increment
  
  # Apply percent space change bound to store-category level (does not apply to drill-downs, so dummy values are used)
  if(jobType=="Tiered"){
    master$PCT_Change_Lower_Limit <- max(0,floor(master$Space*(1-pct_chg_limit)/increment)*increment)
    master$PCT_Change_Upper_Limit <- ceiling(master$Space*(1+pct_chg_limit)/increment)*increment
  } else {
    master$PCT_Change_Lower_Limit <- 0
    master$PCT_Change_Upper_Limit <- master$PCT_of_Space_Upper_Limit
  }

  # Take the max of the lower and min of the upper as the preliminary bounds  
  master$Lower_Limit <- pmax(master$Space.Lower.Limit,master$PCT_Change_Lower_Limit,master$PCT_of_Space_Lower_Limit)  
  master$Upper_Limit <- pmin(master$Space.Upper.Limit,master$PCT_Change_Upper_Limit,master$PCT_of_Space_Upper_Limit)
  
  # Apply exception conditions for sales penetration threshold, exits, and where no sales curve was generated (Enhanced only)
  for(i in 1:nrow(master)){
    master$Sales_Pen[i] <- master$Sales[i]/sum(master$Sales[which(master$Store[i] == master$Store)])
  }
  master$Lower_Limit <- 
    ifelse((master$Exit_Flag=="Exit" | master$Sales_Pen<sls_pen_thresh | (jobType=="Enhanced" & master$Scaled_Alpha_Sales==0)),
           0,
           master$Lower_Limit)
  master$Upper_Limit <- 
    ifelse((master$Exit_Flag=="Exit" | master$Sales_Pen<sls_pen_thresh | (jobType=="Enhanced" & master$Scaled_Alpha_Sales==0)),
           0,
           master$Upper_Limit)

  # END bound-setting
  
  master <- master[c("Store","Climate","VSG","Category","Store_Group_Sales","Store_Group_Profit","Store_Group_Units","Space",
                     "Sales","Scaled_Alpha_Sales","Scaled_Shift_Sales","Scaled_Beta_Sales","Scaled_BP_Sales",
                     "Units","Scaled_Alpha_Units","Scaled_Shift_Units","Scaled_Beta_Units","Scaled_BP_Units",
                     "Profit","Scaled_Alpha_Profit","Scaled_Shift_Profit","Scaled_Beta_Profit","Scaled_BP_Profit",
                     "Lower_Limit","Upper_Limit","Space_to_Fill",
                     "BOH_Dollar","BOH_Units","CC_Count_w_BOH","Profit_Percent","Receipts_Dollar","Receipts_Units")]
  final_out <- list(master,ref) 
  
  return (final_out)
  
}

# Initial parameter setting for curve fitting
# incr_test <- 2.5
# pct_chg_limit_test <- 0.5
# sls_pen_thresh_test <- 0.02
# jobType_test <-"Tiered"
# meth_test <- "Enhanced"

# Read in data as CSV
# master_test <- read.csv(paste0(curr_prod_name,"_Data_Merging_Output.csv"), header=TRUE, sep=",")
# bound_input_test <- read.csv(paste0(curr_prod_name,"_Bound_Input.csv"), header=TRUE, sep=",")

# Call function
# ptm <- proc.time()
# result = curvefitting_boundsetting(master_test,bound_input_test,incr_test,pct_chg_limit_test,sls_pen_thresh_test,jobType_test,meth_test)
# print(proc.time() - ptm)

# str_cat_results = as.data.frame(result[1])
# analytics_reference = as.data.frame(result[2])

# Write output to CSV
# write.csv(str_cat_results,paste0(curr_prod_name,"_Curve_Fitting_Results.csv"),row.names = FALSE)
# write.csv(analytics_reference,paste0(curr_prod_name,"_Analytics_Reference.csv"),row.names = FALSE)