#setwd("C:\\Users\\alison.stern\\Documents\\Kohls\\FO Enhancements\\R Code\\3 R Functions_08.24.2016\\Forecasting")


library(pracma)


Fcst<-read.csv("Forecast_Input.csv")


forecast<-function(Fsct){

Fcst$Estimated_Sales<-Fcst$Future_Est_Sales<-ifelse(Fcst$Recommended_Space<Fcst$Scaled_BP_Sales,Fcst$Recommended_Space*Fcst$Scaled_Alpha_Sales*(erf((Fcst$Scaled_BP_Sales-Fcst$Scaled_Shift_Sales)/(sqrt(2)*Fcst$Scaled_Beta_Sales)))/Fcst$Scaled_BP_Sales,Fcst$Scaled_Alpha_Sales*erf((Fcst$Recommended_Space-Fcst$Scaled_Shift_Sales)/(sqrt(2)*Fcst$Scaled_Beta_Sales)))

Fcst$Estimated_Units<-Fcst$Future_Est_Units<-ifelse(Fcst$Recommended_Space<Fcst$Scaled_BP_Units,Fcst$Recommended_Space*Fcst$Scaled_Alpha_Units*(erf((Fcst$Scaled_BP_Units-Fcst$Scaled_Shift_Units)/(sqrt(2)*Fcst$Scaled_Beta_Units)))/Fcst$Scaled_BP_Units,Fcst$Scaled_Alpha_Units*erf((Fcst$Recommended_Space-Fcst$Scaled_Shift_Units)/(sqrt(2)*Fcst$Scaled_Beta_Units)))

Fcst$Estimated_Profit<-Fcst$Future_Est_Profit<-ifelse(Fcst$Recommended_Space<Fcst$Scaled_BP_Profit,Fcst$Recommended_Space*Fcst$Scaled_Alpha_Profit*(erf((Fcst$Scaled_BP_Profit-Fcst$Scaled_Shift_Profit)/(sqrt(2)*Fcst$Scaled_Beta_Profit)))/Fcst$Scaled_BP_Profit,Fcst$Scaled_Alpha_Profit*erf((Fcst$Recommended_Space-Fcst$Scaled_Shift_Profit)/(sqrt(2)*Fcst$Scaled_Beta_Profit)))

Fcst_out<-Fcst[c("Store","Climate","VSG","Product","Space","Sales","Units","Profit","Recommended_Space","Estimated_Sales","Estimated_Profit","Estimated_Units","Lower_Limit","Upper_Limit")]

return(Fcst_out)

}


Forecast_output<-forecast(Fsct)
write.csv(Forecast_output,"forecast output.csv",row.names = FALSE)