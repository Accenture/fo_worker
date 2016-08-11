
#######-- Forecast code#####

library(MASS)
library(readr)
library(Hmisc)
library(timeDate)
library(gdata)
#install.packages("reshape2")
#install.packages("pracma")
library(pracma)
library(reshape2)
library(data.table)
library(plyr)


setwd("C:/Users/a.rajeshwar.andhe/Desktop/FO")

OIM<-as.data.frame(read_csv("Optimization Input Misses.csv"))

FS<-read_csv("plus fall 2016 fixture counts by brand.csv")

FS1<-melt(FS, id.vars=c("Store","LOC","State","Climate")) #Transporting to columns to rows

FS1$variable<-as.character(FS1$variable)

FS2<-rename.vars(FS1, from=c("variable","value"), to=c("Product","Future_Space")) #Changing column names


trim <- function (x) gsub("^\\s+|\\s+$", "", x) #Function for column trim

FS2$Key<-paste(trim(FS2$Store),trim(FS2$Product)) #Creating Key column for merging

OIM$Key<- paste(trim(OIM$Store),trim(OIM$Product)) #Creating Key column for merging

FS3<-FS2[c("Key","Future_Space")] #Considering only required columns


OIM_FS<-merge(x = OIM, y = FS3, by = "Key", all.x = TRUE) #Left join on Optimization Input Missed by taking "Key" as reference

incsize<-1 #For non incremental space

OIM_FS$Future_Est_Sales<-ifelse(OIM_FS$Future_Space*incsize<OIM_FS$breakpoint,OIM_FS$Future_Space*incsize*OIM_FS$alpha*(erf((OIM_FS$breakpoint-OIM_FS$shift)/(sqrt(2)*OIM_FS$beta)))/OIM_FS$breakpoint,OIM_FS$alpha*erf((OIM_FS$Future_Space*incsize-OIM_FS$shift)/(sqrt(2)*OIM_FS$beta)))

OIM_FS<-OIM_FS[,!names(OIM_FS) %in% c("Key")] #Excluding Key column

#write.csv(OIM_FS,"Future_Est_Sales_Misses_Plus_Forecast_output.csv",row.names = FALSE) # Exporting output file in csv format


#####  Summary Task###########

OIM_FS<-read_csv("Future_Est_Sales_Misses_Plus_Forecast_output.csv")



Summary<-ddply(OIM_FS, .(Product), summarise, 
      Sum_Hist_Space=sum(Space), 
      Max_Hist_Space=max(Space),
      Min_Hist_Space=min(Space), 
      Avg_Hist_Space=mean(Space),
      Sum_Hist_Sales=sum(Sales),
      Sum_New_Space=sum(Future_Space), 
      Max_New_Space=max(Future_Space),
      Min_New_Space=min(Future_Space), 
      Avg_New_Space=mean(Future_Space),
      Sum_New_Sales=sum(Future_Est_Sales)
      
      )

Summ1<-ddply(OIM_FS, .(Store), summarise, 
               Sum_Hist_Space_by_store=sum(Space),
               Sum_Hist_Sales_by_store=sum(Sales),
               Sum_New_Sales_by_store=sum(Future_Est_Sales),
               Sum_New_Space_by_store=sum(Future_Space)
               
)



OIM_FS1<-merge(x = OIM_FS, y = Summ1, by = "Store", all.x = TRUE) 

OIM_FS1$Hist_Pct_BA_Sales<-(OIM_FS1$Sales/OIM_FS1$Sum_Hist_Sales_by_store)

OIM_FS1$Hist_PCT_BA_Space<-(OIM_FS1$Space/OIM_FS1$Sum_Hist_Space_by_store)

OIM_FS1$New_PCT_BA_Sales<-(OIM_FS1$Future_Est_Sales/OIM_FS1$Sum_New_Sales_by_store)

OIM_FS1$New_PCT_BA_Space<-(OIM_FS1$Future_Space/OIM_FS1$Sum_New_Space_by_store)


SU_Pro<-read_csv("C:/Users/a.rajeshwar.andhe/Desktop/FO/5_16_2016/Sales_units_Profit.csv")

SU_Pro1<-SU_Pro[,names(SU_Pro) %in% c("STR_NBR","BRND_SHORT_NM","Sales Units","Profit $")]

SU_Pro1$Key<-paste(trim(SU_Pro1$STR_NBR),trim(SU_Pro1$BRND_SHORT_NM)) #Creating Key column for merging

OIM_FS1$Key<- paste(trim(OIM_FS1$Store),trim(OIM_FS1$Product)) #Creating Key column for merging

OIM_FS2<-merge(x = OIM_FS1, y = SU_Pro1, by = "Key", all.x = TRUE) 

OIM_FS2<-rename.vars(OIM_FS2, from=c("Sales Units","Profit $"), to=c("Sales_Units","Profit"))

#write.csv(OIM_FS2,"Summary_BA_related_info.csv")

Summary2<-ddply(OIM_FS2, .(Product), summarise, 
               Sum_Hist_Sales_units=sum(Sales_Units), 
               Sum_Hist_Profit=sum(Profit)
)

Summary3<-merge(x = Summary, y = Summary2, by = "Product", all.x = TRUE) 

Summary3$Space_change<-(Summary3$Sum_New_Space-Summary3$Sum_Hist_Space)

Summary3$Est_Sales_Change<-(Summary3$Sum_New_Sales-Summary3$Sum_Hist_Sales)

Summary3$Space_PCT_change<-((Summary3$Sum_New_Space-Summary3$Sum_Hist_Space)/Summary3$Sum_Hist_Space)

Summary3$Est_Sales_PCT_Change<-((Summary3$Sum_New_Sales-Summary3$Sum_Hist_Sales)/Summary3$Sum_Hist_Sales)

Summary3$Hist_Productivity<-(Summary3$Sum_Hist_Sales/Summary3$Sum_Hist_Space)

Summary3$New_Productivity<-(Summary3$Sum_New_Sales/Summary3$Sum_New_Space)

Summary3$Productivity_change<-(Summary3$New_Productivity-Summary3$Hist_Productivity)

Summary3$Productivity_PCT_change<-((Summary3$New_Productivity-Summary3$Hist_Productivity)/Summary3$Hist_Productivity)



Input_SU<-read_csv("C:/Users/a.rajeshwar.andhe/Desktop/FO/5_17_2016/Inputs_Sales_units.csv")
Input_SU$Key<-paste(trim(Input_SU$Store),trim(Input_SU$BRND_SHORT_NM)) #Creating Key column for merging
Input_SU<-Input_SU[,!names(Input_SU) %in% c("Store","Product","BRND_SHORT_NM")]

OIM_FS3<-merge(x = OIM_FS2, y = Input_SU, by = "Key", all.x = TRUE) 


Input_Profits<-read_csv("C:/Users/a.rajeshwar.andhe/Desktop/FO/5_17_2016/Inputs_Profit.csv")

Input_Profits$Key<-paste(trim(Input_Profits$Store),trim(Input_Profits$BRND_SHORT_NM)) #Creating Key column for merging
Input_Profits<-Input_Profits[,!names(Input_Profits) %in% c("Store","Product","BRND_SHORT_NM")]

OIM_FS4<-merge(x = OIM_FS3, y = Input_Profits, by = "Key", all.x = TRUE) 


OIM_FS4$Future_Est_SU<-ifelse(OIM_FS4$Future_Space*incsize<OIM_FS4$Scaled_Sales_units_BP,OIM_FS4$Future_Space*incsize*OIM_FS4$Scaled_Sales_units_alpha*(erf((OIM_FS4$Scaled_Sales_units_BP-OIM_FS4$Scaled_Sales_units_Shift)/(sqrt(2)*OIM_FS4$Scaled_Sales_units_beta)))/OIM_FS4$Scaled_Sales_units_BP,OIM_FS4$Scaled_Sales_units_alpha*erf((OIM_FS$Future_Space*incsize-OIM_FS4$Scaled_Sales_units_Shift)/(sqrt(2)*OIM_FS4$Scaled_Sales_units_beta)))

OIM_FS4$Future_Est_Profits<-ifelse(OIM_FS4$Future_Space*incsize<OIM_FS4$Scaled_Profits_BP,OIM_FS4$Future_Space*incsize*OIM_FS4$Scaled_Profits_alpha*(erf((OIM_FS4$Scaled_Profits_BP-OIM_FS4$Scaled_Profits_Shift)/(sqrt(2)*OIM_FS4$Scaled_Profits_beta)))/OIM_FS4$Scaled_Profits_BP,OIM_FS4$Scaled_Profits_alpha*erf((OIM_FS$Future_Space*incsize-OIM_FS4$Scaled_Profits_Shift)/(sqrt(2)*OIM_FS4$Scaled_Profits_beta)))

Summary4<-ddply(OIM_FS4, .(Product), summarise, 
                Sum_New_Sales_units=sum(Future_Est_SU), 
                Sum_New_Profit=sum(Future_Est_Profits)
)


Summary5<-merge(x = Summary3, y = Summary4, by = "Product", all.x = TRUE) 

Summary5$Est_SU_Change<-(Summary5$Sum_New_Sales_units-Summary5$Sum_Hist_Sales_units)

Summary5$Est_Profits_Change<-(Summary5$Sum_New_Profit-Summary5$Sum_Hist_Profit)

Summary5$Est_SU_PCT_Change<-((Summary5$Sum_New_Sales_units-Summary5$Sum_Hist_Sales_units)/Summary5$Sum_Hist_Sales_units)

Summary5$Est_Profits_PCT_Change<-((Summary5$Sum_New_Profit-Summary5$Sum_Hist_Profit)/Summary5$Sum_Hist_Profit)


OIM_FS4$Space_Change<-(OIM_FS4$Future_Space-OIM_FS4$Space)
OIM_FS4$Sales_change<-(OIM_FS4$Future_Est_Sales-OIM_FS4$Sales)

Summary6<-ddply(OIM_FS4, .(Product), summarise, 
                No_Stores_With_no_Hist_Space=length(Product[Space == 0]),
                No_Stores_With_Hist_Space_No_Fut_Space=length(Product[Space>0 & Future_Space==0]),
                No_Stores_Increased=length(Product[Space_Change>0]),
                No_Stores_Decreased=length(Product[Space_Change!=0])
                
                )


for (i in (1:length(unique(OIM_FS4$Product)))){
  Summary6$Sum_Space_change_With_no_Hist_Space[i]= sum(OIM_FS4$Space_Change[which(OIM_FS4$Space==0 & OIM_FS4$Product %in% OIM_FS4$Product[i])])
}

for (i in (1:length(unique(OIM_FS4$Product)))){
  Summary6$Sum_Space_change_With_Hist_Space_No_Fut_Space[i]= sum(OIM_FS4$Space_Change[which(OIM_FS4$Space > 0 & OIM_FS4$Future_Space==0 & OIM_FS4$Product %in% OIM_FS4$Product[i])])
}

for (i in (1:length(unique(OIM_FS4$Product)))){
  Summary6$Sum_Space_change_Stores_Increased[i]= sum(OIM_FS4$Space_Change[which(OIM_FS4$Space_Change > 0  & OIM_FS4$Product %in% OIM_FS4$Product[i])])
}

for (i in (1:length(unique(OIM_FS4$Product)))){
  Summary6$Sum_Space_change_Stores_Decreased[i]= sum(OIM_FS4$Space_Change[which(OIM_FS4$Space_Change != 0  & OIM_FS4$Product %in% OIM_FS4$Product[i])])
}



for (i in (1:length(unique(OIM_FS4$Product)))){
  Summary6$Sum_Sales_change_With_no_Hist_Space[i]= sum(OIM_FS4$Sales_change[which(OIM_FS4$Space==0 & OIM_FS4$Product %in% OIM_FS4$Product[i])])
}

for (i in (1:length(unique(OIM_FS4$Product)))){
  Summary6$Sum_Sales_change_With_Hist_Space_No_Fut_Space[i]= sum(OIM_FS4$Sales_change[which(OIM_FS4$Space > 0 & OIM_FS4$Future_Space==0 & OIM_FS4$Product %in% OIM_FS4$Product[i])])
}

for (i in (1:length(unique(OIM_FS4$Product)))){
  Summary6$Sum_Sales_change_Stores_Increased[i]= sum(OIM_FS4$Sales_change[which(OIM_FS4$Space_Change > 0  & OIM_FS4$Product %in% OIM_FS4$Product[i])])
}

for (i in (1:length(unique(OIM_FS4$Product)))){
  Summary6$Sum_Sales_change_Stores_Decreased[i]= sum(OIM_FS4$Sales_change[which(OIM_FS4$Space_Change != 0  & OIM_FS4$Product %in% OIM_FS4$Product[i])])
}



Summary7<-merge(x = Summary5, y = Summary6, by = "Product", all.x = TRUE) 

