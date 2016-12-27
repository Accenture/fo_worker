rm(list=ls())

library(rJava)
library(xlsxjars)
library(XLConnectJars)
library(xlsx)
library(XLConnect)
library(excel.link)
library(pracma)
library(hydroGOF)
library(grDevices)
library(ggplot2)

# Read in curve-fitting outputs and set working directory
curr_prod_name <- "Plus"
setwd(paste0("C:\\Users\\saurabh.a.trivedi\\Desktop\\Protyping Code\\Mens Athletic Input template Updated\\",curr_prod_name))
ref_data<-read.csv(paste0(curr_prod_name,"_Analytics_Reference.csv"),header=TRUE,sep=",")
master_data<-read.csv(paste0(curr_prod_name,"_Curve_Fitting_Results.csv"),header=TRUE,sep=",")

# Set parameters for visualizing distribution markers
bucket_width <- 0.5
bucket_threshold <- 25

# Declare mround function for rounding to the nearest increment
mround <- function(x,base){ 
  base*round(x/base) 
}

# Loop through each metric
for(target in c("Sales","Profit","Units")){

  # Select the category-store group unscaled coefficient information for the current metric
  reference <- ref_data[which(ref_data$Metric == target),]
  reference[with(reference, order(Category,Store_Group)), ]

  # Loop through each category-store group
  for(l in 1:nrow(reference)){
    
    # Retrieve category-store group information
    category <- reference[l,"Category"]
    store_group <- reference[l,"Store_Group"]
    category_sg <- paste0(category,"-",store_group)
    
    # Retrieve unscaled curve information
    alpha <- reference[l,"Unscaled_Alpha"]
    beta <- reference[l,"Unscaled_Beta"]
    shift <- reference[l,"Unscaled_Shift"]
    bp <- reference[l,"Unscaled_BP"]
    bp_productivity <- reference[l,"Unscaled_BP_Prod"]
    
    # Select data points for store-categories in the current category-store group
    data<-master_data[which(master_data$Category == category & master_data[,paste0("Store_Group_",target)] == store_group),]
    
    # Prepare points on the full S curve for a relevant range of space values
    max_space_graph <- mround(ceil(max(data$Space) + bucket_width * 4), bucket_width)
    pdat <- data.frame(Space=seq(0,max_space_graph,bucket_width),target=unlist(sapply(seq(0,max_space_graph,bucket_width), function(Space) (alpha*erf((Space-shift)/(sqrt(2)*beta))), simplify=F)))
    names(pdat)[2]<-target
    
    # Prepare distribution markers to illustrate the distribution of points in each space bucket
    pdat$Lower_Bucket_Center<-pdat$Space - bucket_width/2
    pdat$Upper_Bucket_Center<-pdat$Space + bucket_width/2
    for(i in 1:nrow(pdat)){
      pdat$Count[i]<-length(which(data$Space>=pdat$Lower_Bucket_Center[i] & data$Space<pdat$Upper_Bucket_Center[i]))  
      pdat$Average[i]<-ifelse(pdat$Count[i]>bucket_threshold,mean(data[which(data$Space>pdat$Lower_Bucket_Center[i] & data$Space<pdat$Upper_Bucket_Center[i]),][,target]),NA)
      pdat$percentile_25[i]<-ifelse(pdat$Count[i]>bucket_threshold,quantile(data[which(data$Space>pdat$Lower_Bucket_Center[i] & data$Space<pdat$Upper_Bucket_Center[i]),][,target],0.25),NA)
      pdat$percentile_75[i]<-ifelse(pdat$Count[i]>bucket_threshold,quantile(data[which(data$Space>pdat$Lower_Bucket_Center[i] & data$Space<pdat$Upper_Bucket_Center[i]),][,target],0.75),NA)
    }

    # Full S Curve plot
    png(paste0(category_sg,".png"), height=8, width=12, res=250, pointsize=100,units="cm")
    options(scipen=10000)
    testp<-pdat$Average
    testp[is.na(testp)] <- ""
    if(all(testp=="")){
      print(ggplot(data, aes_string(x=colnames(data)[6], y=target)) +
              stat_sum(aes(size = NULL), colour = "blue") +
              theme_bw() +
              geom_line(data = pdat, colour = "red") +
              ggtitle(category_sg))
    } else {
      print(ggplot(data,aes_string(x=colnames(data)[6],y=target)) +
              stat_sum(aes(size = NULL), colour = "blue") +
              theme_bw() +
              geom_line(data = pdat, colour = "red") +
              geom_point(data = pdat, aes(x = Space, y = percentile_25), colour = "black",  shape = 15, size = 3) +
              geom_point(data = pdat, aes(x = Space, y = percentile_75), colour = "black",  shape = 15, size = 3) + 
              geom_point(data = pdat, aes(x = Space, y = Average), colour = "yellow",  shape = 15, size = 3) + 
              ggtitle(category_sg))
    }
    
    dev.off()
    
    #Exporting full S curve graphs into an excel file one below the other
    if(l == 1){
      wb <- xlsx::createWorkbook()      
      sheet <- xlsx::createSheet(wb, paste0("All_Graph_",target))  
    }
    addPicture(paste0(category_sg,".png"), sheet, scale = 1, startRow = (l-1)*16+1, startColumn = 1)
    xlsx::saveWorkbook(wb, paste0("All_Graph_",target,".xlsx"))
    
    # Replace full S curve y values with y values from S curve with linear assumption
    pdat[,paste0(target)] <- ifelse(pdat$Space<bp,pdat$Space*bp_productivity,alpha*erf((pdat$Space-shift)/(sqrt(2)*beta)))
    
    # S Curve with Linear Assumption below the Break Point plot
    png(paste0(category_sg,".png"), height=8, width=12, res=250, pointsize=100,units="cm")
    options(scipen=10000)
    testq<-pdat$Average
    testq[is.na(testq)] <- ""
    if(all(testq=="")){
      print(ggplot(data,aes_string(x=colnames(data)[6],y=target)) + 
              stat_sum(aes(size = NULL), colour = "blue") + 
              theme_bw() + 
              geom_line(data = pdat, colour = "red") + 
              ggtitle(category_sg))
    } else {
      print(ggplot(data,aes_string(x=colnames(data)[6],y=target)) + 
              stat_sum(aes(size = NULL), colour = "blue") + 
              theme_bw() +
              geom_line(data = pdat, colour = "red") + 
              geom_point(data = pdat,aes(x = Space, y = percentile_25), colour = "black", shape = 15, size = 3) +
              geom_point(data = pdat,aes(x = Space, y = percentile_75), colour = "black", shape = 15, size = 3) +
              geom_point(data = pdat,aes(x = Space, y = Average), colour = "yellow", shape = 15, size = 3) + 
              ggtitle(category_sg))  
    }
    
    dev.off()
    
    #Exporting S Curve with Linear Assumption graphs into an excel file one below the other
    if(l == 1){
      wb1 <- xlsx::createWorkbook()      
      sheet1 <- xlsx::createSheet(wb1, paste0("All Graphs with LA_",target))  
    }
    addPicture(paste0(category_sg,".png"), sheet1, scale = 1, startRow = (l-1)*16+1, startColumn = 1)
    xlsx::saveWorkbook(wb1, paste0("All Graphs with LA_",target,".xlsx"))
    files_to_remove <- list.files(pattern=".png", full.name=T)
    file.remove(files_to_remove)
  }

}


