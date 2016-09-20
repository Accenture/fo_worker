#stime<-system.time({
rm(list=ls())

library(gdata)
library(data.table)
library(sqldf)
library(tidyr)
library(reshape2)

# setwd("/Users/kenneth.l.sylvain/Sites/kohls/fo_worker/rpy2_POC/DataMerging")

#mydir<-("C:\\Users\\alison.stern\\Documents\\Kohls\\FO Enhancements\\R Code\\3 R Functions_08.24.2016\\Data Merging")


#------Historical Performance-------------

# Hist_perf<-read.csv("transactions_data.csv",header = F) #Reading python input file of Historical performance 

# HSCI<-read.csv("fixture_data.csv",header = F) #Reading python input file of Historical space and climate information

# Future_Space_Entry_Data <-read.csv("Entry.Future Space.csv",header=TRUE,sep=",",check.names=FALSE)

# Brand_Exit<-read.csv("exit_data.csv",header = F) #Reading python input file of Exit data

# optimType ="Tiered"

# function assumes dataframe is coming in with 2 header rows. Function will deal with those two rows appropriately
Data_Merge<-function(Hist_perf,HSCI,Future_Space_Entry_Data,Brand_Exit,optimType){

colnames(Hist_perf)=as.character(unlist(Hist_perf[2,])) #Assigning columns names

Hist_perf=Hist_perf[-2,] #Removing column names row

Hist_perf1<-t(Hist_perf) # Transposing the data

colnames(Hist_perf1)<-Hist_perf1[1,] #Assigning column names

Hist_perf1<-Hist_perf1[-1,] #Removing column names row

Hist_perf1<-as.data.frame(Hist_perf1) # Converting into data frame

Hist_perf1<-rename.vars(Hist_perf1,from=c("Store"), to=c("Cat")) # Renaming variables

# Created a function to fill blanks with above cell value
fillTheBlanks <- function(x, missing=""){
  rle <- rle(as.character(x))
  empty <- which(rle$value==missing)
  rle$values[empty] <- rle$value[empty-1] 
  inverse.rle(rle)
}

Hist_perf1$Cat<-fillTheBlanks(Hist_perf1$Cat) #Filling the blank rows with above cell
Hist_perf1<-setDT(Hist_perf1, keep.rownames = TRUE)[] #Creating rowindex as column in data frame
Hist_perf1<-melt(Hist_perf1,measure.vars = colnames(Hist_perf1)[-(1:2)]) #Reshaping the data
trim <- function (x) gsub("^\\s+|\\s+$", "", x) # created function for removing leading and trailing spaces
Hist_perf1$rn<-trim(Hist_perf1$rn)
Hist_perf1<-dcast(Hist_perf1,Cat+variable~rn,value.var = "value") #long to wide format
Hist_perf1<-rename.vars(Hist_perf1,from=c("variable"),to=c("Store")) #Renaming variables
#---------Historical space and Climate information--------------

colnames(HSCI)=trim(as.character(unlist(HSCI[1,]))) #Assigning columns names
HSCI1<-HSCI[-1,] #removing not required row
HSCI1<-HSCI1[-1,] #removing not required row
HSCI1<-melt(HSCI1,measure.vars = colnames(HSCI1)[-(1:3)]) #Reshaping the data
HSCI1<-rename.vars(HSCI1,from=c("variable"),to=c("Cat")) #Renaming variables
# Merging between Historical performance input and Historical space and climate info
Hist_perf_space<-sqldf("select a.*,b.Climate,b.VSG,b.value as space from Hist_perf1 a left join HSCI1 b on a.Store=b.Store and a.Cat=b.Cat")
Hist_perf_space<-rename.vars(Hist_perf_space,from = c("Cat","space","Sales $","Sales Units","Profit $"), to=c("Product","Space","Sales","Units","Profit"))
big_master_data<-Hist_perf_space[c("Store","Climate","VSG","Product","Space","Sales","Units","Profit","BOH $","BOH Units","CC Count w/ BOH","Profit %","Receipts  $","Receipts Units")]


#Checking for the availability of other required files
# a)Brand Entry file
if(optimType =="Tiered"){
  # Whether or not user wants to upload future BA space - if not, target BA space is the same as historical BA space 
  if(is.null(Future_Space_Entry_Data)==TRUE){
    Future_Space_Entry_Data<-big_master_data
    for(s in (1:nrow(Future_Space_Entry_Data))){
      #kb change made to column reference from Future_Space_Entry_Data$Future_Space[s] to the following
      Future_Space_Entry_Data[s,"Future Space"]<-sum(Future_Space_Entry_Data$Space[which(Future_Space_Entry_Data$Store %in% Future_Space_Entry_Data$Store[s])])
    }
    Future_Space_Entry_Data<-Future_Space_Entry_Data[!duplicated(Future_Space_Entry_Data[c("Store", "Future Space")]),]
    Future_Space_Entry_Data<-Future_Space_Entry_Data[c("Store","Future Space")]
    #kb changed column reference
    Future_Space_Entry_Data["Entry Space"]<-0
    Future_Space_Entry_Data$VSG<-NA
    Future_Space_Entry_Data$Climate<-NA
    Future_Space_Entry_Data<-Future_Space_Entry_Data[c("Store","Climate","VSG","Future Space","Entry Space")]
  } else {
    Future_Space_Entry_Data<-Future_Space_Entry_Data[-1,]
    names(Future_Space_Entry_Data)<-gsub(" ","_",names(Future_Space_Entry_Data))
  }
  #Whether or not user has any brand exit information - if not, there will not be any product/stores with LB/UB = 0 due to brand exit
  if(is.null(Brand_Exit)==TRUE){
    Brand_Exit<-big_master_data
    Brand_Exit$Exit_Flag<-0
    Brand_Exit<-Brand_Exit[c("Store","Product","Exit_Flag")]
    Brand_Exit<-Brand_Exit[!duplicated(Brand_Exit[c("Store", "Product")]),]
    
  } else {
    colnames(Brand_Exit)=trim(as.character(unlist(Brand_Exit[1,]))) #Assigning columns names
    Brand_Exit<-Brand_Exit[-1,] #removing not required row
    Brand_Exit<-Brand_Exit[-1,]
    Brand_Exit<-gather(Brand_Exit,Product,Store)
    Brand_Exit<-Brand_Exit[!(Brand_Exit$Store==""),]
    Brand_Exit$Exit_Flag<-"Exit"
  }
} else {
  #target BA space is the same as historical BA space 
  if(is.null(Future_Space_Entry_Data)==TRUE){
    #names(Future_Space_Entry_Data)<-gsub(" ","_",names(Future_Space_Entry_Data))
    Future_Space_Entry_Data<-big_master_data
    for(s in (1:nrow(Future_Space_Entry_Data))){
      Future_Space_Entry_Data$Future_Space[s]<-sum(Future_Space_Entry_Data$Space[which(Future_Space_Entry_Data$Store %in% Future_Space_Entry_Data$Store[s])])
    }
    Future_Space_Entry_Data<-Future_Space_Entry_Data[!duplicated(Future_Space_Entry_Data[c("Store", "Future Space")]),]
    Future_Space_Entry_Data<-Future_Space_Entry_Data[c("Store","Future Space")]
    #kb changed column reference
    Future_Space_Entry_Data["Entry Space"]<-0
    Future_Space_Entry_Data$VSG<-NA
    Future_Space_Entry_Data$Climate<-NA
    Future_Space_Entry_Data<-Future_Space_Entry_Data[c("Store","Climate","VSG","Future Space","Entry Space")]
  } else {
    #kb commented out to maintain consistency in naming
    #names(Future_Space_Entry_Data)<-gsub(" ","_",names(Future_Space_Entry_Data))
    Future_Space_Entry_Data<-Future_Space_Entry_Data[-1,]
    #kb changed column reference
    Future_Space_Entry_Data["Entry Space"]<-0
  }
  #there will not be any product/stores with LB/UB = 0 due to brand exit
  Brand_Exit<-big_master_data
  Brand_Exit$Exit_Flag<-0
  Brand_Exit<-Brand_Exit[c("Store","Product","Exit_Flag")]
  Brand_Exit<-Brand_Exit[!duplicated(Brand_Exit[c("Store", "Product")]),]
}

big_master_data<-merge(big_master_data,Brand_Exit,by=c("Store","Product"),all.x=TRUE)
big_master_data$Exit_Flag<-ifelse(is.na(big_master_data$Exit_Flag)==TRUE,0,big_master_data$Exit_Flag)
big_master_data<-merge(big_master_data,Future_Space_Entry_Data[,which(names(Future_Space_Entry_Data) %in% c("Store","Future Space","Entry Space"))],by="Store",all.x=TRUE)
print(str(big_master_data))
big_master_data<-big_master_data[c("Store","Climate","VSG","Product","Space","Sales","Units","Profit","Exit_Flag","Future Space"
                                  ,"Entry Space","BOH $","BOH Units","CC Count w/ BOH","Profit %","Receipts  $","Receipts Units")]


return(big_master_data)

}

#output<-Data_merge(Hist_perf,HSCI,Future_Space_Entry_Data,Brand_Exit, optimType)
#write.csv(output,"output.csv",row.names = FALSE)
