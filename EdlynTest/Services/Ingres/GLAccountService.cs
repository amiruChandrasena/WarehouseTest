using Abstractions.ServiceInterfaces;
using Common;
using Microsoft.Extensions.Configuration;
using Models;
using Services.Ingres.SQLResources;
using System;
using System.Collections.Generic;
using System.Data.Odbc;
using System.Globalization;
using System.Linq;
using System.Text;
using Westwind.Utilities.Data;

namespace Services.Ingres
{
    public class GLAccountService : IGLAccountService
    {
        private readonly string connectionString;

        public GLAccountService(IConfiguration configuration)
        {
            connectionString = configuration.GetConnectionString("IngresDatabase");
        }

        public TransactionWrapper GetJobSettingsByParamName(string paramName)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            GLSettingsModel setting = new GLSettingsModel();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = JobSQL.ResourceManager.GetString("GetJobSettingsByParamName");

                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@ParamName", OdbcType.VarChar).Value = paramName;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            dynamic dReader = new DynamicDataReader(reader);

                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    setting = new GLSettingsModel
                                    {
                                        Comment = dReader.comment,
                                        ParamName = dReader.param_name,
                                        ParamValChar = dReader.param_val_char,
                                        Status = dReader.status,
                                        WhoChanged = dReader.who_changed
                                    };

                                    if (dReader.param_val_float != null)
                                    {
                                        setting.ParamValFloat = dReader.param_val_float;
                                    }

                                    if (dReader.param_val_int != null)
                                    {
                                        setting.ParamValInt = dReader.param_val_int;
                                    }
                                    if (dReader.when_changed != null)
                                    {
                                        DateTime whenChangedDate = dReader.when_changed;
                                        setting.WhenChanged = whenChangedDate.ToShortDateString();
                                    }
                                }
                            }
                            else
                            {
                                wrapper.IsSuccess = false;
                                wrapper.Messages.Add("GetJobSettingsByParamName: No setting found for " + paramName);
                                return wrapper;
                            }
                        }
                    }

                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetJobSettingsByParamName: " + e.Message);
                    return wrapper;
                }
            }

            wrapper.IsSuccess = true;
            wrapper.ResultSet.Add(setting);
            return wrapper;
        }

        public TransactionWrapper GetGLBatchHeaderByBatchNo(int batchNo)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = GLAccountSQL.ResourceManager.GetString("GetGLBatchHeaderByBatchNo");

                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@BatchNo", OdbcType.Int).Value = batchNo;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            dynamic dReader = new DynamicDataReader(reader);

                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    GLBatchHeaderModel header = new GLBatchHeaderModel
                                    {
                                        BatchNo = dReader.Batch_No,
                                        Source = dReader.source,
                                        Status = dReader.status,
                                        Description = dReader.description
                                    };
                                    wrapper.ResultSet.Add(header);
                                }
                            }
                        }
                    }
                    wrapper.IsSuccess = true;
                    return wrapper;
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetGLBatchHeaderByBatchNo : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper SaveJnlGLTransactions(GLBatchHeaderModel glBatch)
        {
            string queryString = "";
            int maxJnlGLNo = 0;

            TransactionWrapper wrapper = new TransactionWrapper();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    #region "fetch a unique number"

                    queryString = GLAccountSQL.ResourceManager.GetString("UpdateGLBatchUniqueKey");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        int rowsAffected = command.ExecuteNonQuery();
                    }

                    queryString = GLAccountSQL.ResourceManager.GetString("GetMaxGLBatchUniqueKey");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                maxJnlGLNo = reader.GetInt32(0);
                            }
                        }
                    }
                    #endregion

                    #region "validate"

                    // check that we have a valid batch
                    if (maxJnlGLNo > 0)
                    {
                        glBatch.BatchNo = maxJnlGLNo;
                    }
                    else
                    {
                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add("SaveJnlGLTransactions : No batch number specified");
                        return wrapper;
                    }

                    if (glBatch.Source == "")
                    {
                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add("SaveJnlGLTransactions : No Source specified");
                        return wrapper;
                    }
                    else
                    {
                        List<GLJnlGLSourceModel> sourceDetList = new List<GLJnlGLSourceModel>();
                        queryString = GLAccountSQL.ResourceManager.GetString("GetSourceRecordBySource");

                        using (OdbcCommand command = new OdbcCommand(queryString, connection))
                        {
                            command.Parameters.Add("@Source", OdbcType.VarChar).Value = glBatch.Source;

                            using (OdbcDataReader reader = command.ExecuteReader())
                            {
                                dynamic dReader = new DynamicDataReader(reader);

                                if (reader.HasRows)
                                {
                                    while (reader.Read())
                                    {
                                        GLJnlGLSourceModel sourceDet = new GLJnlGLSourceModel
                                        {
                                            Source = dReader.source,
                                            SourceDesc = dReader.description,
                                        };

                                        sourceDetList.Add(sourceDet);
                                    }
                                }
                            }
                        }

                        if (sourceDetList.Count == 0)
                        {
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add("SaveJnlGLTransactions : Source code does not exist");
                            return wrapper;
                        }
                    }

                    #endregion

                    #region "saveit"

                    wrapper = SaveGlBatch(glBatch);
                    if (wrapper.IsSuccess == false)
                    {
                        return wrapper;
                    }

                    glBatch = wrapper.ResultSet[0] as GLBatchHeaderModel;

                    #endregion

                    #region closeit

                    wrapper = GetGLBatchHeaderByBatchNo(glBatch.BatchNo);
                    if (wrapper.IsSuccess == false)
                    {
                        return wrapper;
                    }

                    GLBatchHeaderModel batchHeader = new GLBatchHeaderModel();
                    if (wrapper.ResultSet.Count > 0)
                    {
                        batchHeader = wrapper.ResultSet[0] as GLBatchHeaderModel;
                    }
                    else
                    {
                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add("SaveJnlGLTransactions: No batch header found for batch no " + glBatch.BatchNo.ToString());
                    }

                    switch (batchHeader.Status)
                    {
                        case "S":
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add("Cannot close standing journals");
                            break;
                        case "C":
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add("This batch has already been closed");
                            break;
                        case "D":
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add("Cannot make changes to a batch that has been deleted");
                            break;
                        case "P":
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add("Cannot make changes to a batch that has been posted");
                            break;
                        default:
                            wrapper.IsSuccess = true;
                            break;
                    }

                    if (wrapper.IsSuccess == false)
                    {
                        return wrapper;
                    }

                    wrapper = UpdateGlBatchHeaderStatus(glBatch.Originator, "C", glBatch.BatchNo);
                    if (wrapper.IsSuccess == false)
                    {
                        return wrapper;
                    }

                    #endregion

                    #region postit

                    // check intercompany approval
                    wrapper = GetInterCompanyApproval(glBatch.BatchNo);
                    if (wrapper.IsSuccess == false)
                    {
                        return wrapper;
                    }

                    // check financial period
                    int finPeriodCount = 0;
                    wrapper = GetFinPeriodZeroCount(ref finPeriodCount, glBatch.BatchNo);
                    if (wrapper.IsSuccess == false)
                    {
                        return wrapper;
                    }
                    else if (finPeriodCount > 0)
                    {
                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add("Financial period cannot be zero");
                        return wrapper;
                    }

                    // check costYear and costPeriod 
                    int costYear = 0;
                    int costPeriod = 0;

                    wrapper = CheckIfCostYearAndPeriodLocked(ref costYear, ref costPeriod, glBatch.BatchNo);
                    if (wrapper.IsSuccess == false)
                    {
                        return wrapper;
                    }
                    else if (costYear != 0 && costPeriod != 0)
                    {
                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add("Batch " + glBatch.BatchNo.ToString() + " is for cost year" + costYear.ToString() + ", Period " + costPeriod.ToString() + " which is locked");
                        return wrapper;
                    }

                    // check active accounts
                    for (int i = 0; i < glBatch.JnlDetails.Count; i++)
                    {
                        for (int j = 0; j < glBatch.JnlDetails[i].JnlGLDetails.Count; j++)
                        {
                            string status = "";
                            wrapper = CheckActiveAccount(ref status, glBatch.JnlDetails[i].JnlGLDetails[j].GLAccount);
                            if (wrapper.IsSuccess == false)
                            {
                                return wrapper;
                            }
                            else if (status != "A")
                            {
                                wrapper.IsSuccess = false;
                                wrapper.Messages.Add(glBatch.JnlDetails[i].JnlGLDetails[j].GLAccount + " is not an Active account, you will not be able to post this batch unless the status of this account is changed.");
                                return wrapper;
                            }
                        }
                    }

                    // validate Inter EOY
                    wrapper = GetEoyHistoryList(glBatch.BatchNo);
                    if (wrapper.IsSuccess == false)
                    {
                        return wrapper;
                    }

                    List<EoyHistoryModel> eoyList = wrapper.ResultSet[0] as List<EoyHistoryModel>;
                    if (eoyList.Count > 1)
                    {
                        for (int i = 1; i < eoyList.Count; i++)
                        {
                            if (eoyList[0].ProcYear != eoyList[i].ProcYear)
                            {
                                wrapper.IsSuccess = false;
                                wrapper.Messages.Add("Intercompany journals with different cost year are present. Please process EOY for all companies involved before posting the journal");
                                return wrapper;
                            }
                        }
                    }

                    // dont post batch if out of balance
                    decimal balance = 0.00m;
                    for (int i = 0; i < glBatch.JnlDetails.Count; i++)
                    {
                        for (int j = 0; j < glBatch.JnlDetails[i].JnlGLDetails.Count; j++)
                        {
                            if (glBatch.JnlDetails[i].JnlGLDetails[j].JnlCR == 0 && glBatch.JnlDetails[i].JnlGLDetails[j].JnlDB == 0)
                            {
                                balance += Convert.ToDecimal(glBatch.JnlDetails[i].JnlGLDetails[j].JnlAmount);
                            }
                            else
                            {
                                balance += Convert.ToDecimal(glBatch.JnlDetails[i].JnlGLDetails[j].JnlCR - glBatch.JnlDetails[i].JnlGLDetails[j].JnlDB);
                            }
                        }
                    }

                    if (Math.Abs(decimal.Round(balance, 2, MidpointRounding.AwayFromZero)) > 0.02m)
                    {
                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add("Entry not balanced by " + decimal.Round(balance, 2, MidpointRounding.AwayFromZero).ToString() + ". Batch is not posted");
                        wrapper.ResultSet.Clear();
                        wrapper.ResultSet.Add(glBatch);
                        return wrapper;
                    }

                    // update retained

                    wrapper = UpdateRetained(glBatch);
                    if (wrapper.IsSuccess == false)
                    {
                        return wrapper;
                    }
                    else
                    {
                        glBatch = wrapper.ResultSet[0] as GLBatchHeaderModel; // UpdateRetained may add extra GL details
                    }

                    wrapper = UpdateGlBatchHeaderStatus(glBatch.Originator, "P", glBatch.BatchNo);
                    if (wrapper.IsSuccess == false)
                    {
                        return wrapper;
                    }

                    // update balance for all details
                    for (int i = 0; i < glBatch.JnlDetails.Count; i++)
                    {
                        for (int j = 0; j < glBatch.JnlDetails[i].JnlGLDetails.Count; j++)
                        {
                            wrapper = UpdateBalance(glBatch.JnlDetails[i].FinYear,
                                                    glBatch.JnlDetails[i].FinPeriod,
                                                    glBatch.JnlDetails[i].JnlGLDetails[j].GLAccount,
                                                    Convert.ToDecimal(glBatch.JnlDetails[i].JnlGLDetails[j].JnlAmount),
                                                    glBatch.JnlDetails[i].JnlGLDetails[j].Quantity);
                            if (wrapper.IsSuccess == false)
                            {
                                return wrapper;
                            }
                        }
                    }
                    #endregion

                    wrapper.IsSuccess = true;
                    wrapper.ResultSet.Add(glBatch);
                    return wrapper;
                }
                catch (Exception e)
                {
                    wrapper.ResultSet.Add(glBatch);
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add(e.ToString());
                    return wrapper;
                }
            }

        }

        public TransactionWrapper UpdateRetained(GLBatchHeaderModel glBatch)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            string balanceAccount = "";
            GlAccountMasterModel glAccountMaster = null;
            EoyHistoryModel eoyHistory = null;

            wrapper = GetGlAccountMaster(glBatch.JnlDetails[0].JnlGLDetails[0].GLAccount);
            if (wrapper.IsSuccess == false)
            {
                return wrapper;
            }
            else if (wrapper.ResultSet.Count > 0)
            {
                glAccountMaster = wrapper.ResultSet[0] as GlAccountMasterModel;
            }

            if (glAccountMaster != null)
            {
                wrapper = GetEoyHistoryByCompanyCodeAndYear(glAccountMaster.CompanyCode, glBatch.JnlDetails[0].FinYear);
            }
            else
            {
                wrapper = GetEoyHistoryByCompanyCodeAndYear("", glBatch.JnlDetails[0].FinYear);
            }

            if (wrapper.IsSuccess == false)
            {
                return wrapper;
            }
            else if (wrapper.ResultSet.Count > 0)
            {
                eoyHistory = wrapper.ResultSet[0] as EoyHistoryModel;
            }

            if (eoyHistory != null)
            {
                if (eoyHistory.GlBatchNo == glBatch.BatchNo)
                {
                    eoyHistory.Posted = 1;
                    wrapper = UpdateOrInsertEoyHistory(eoyHistory);
                    return wrapper;

                }
                else
                {
                    string companyCode = "";
                    if (glAccountMaster != null)
                    {
                        companyCode = glAccountMaster.CompanyCode;
                    }
                    wrapper = GetJobSettingsByParamName("CB_EOY_GL_" + companyCode);
                    if (wrapper.IsSuccess == false)
                    {
                        return wrapper;
                    }

                    GLSettingsModel settings = wrapper.ResultSet[0] as GLSettingsModel;
                    balanceAccount = settings.ParamValChar;
                }
            }
            else
            {
                wrapper.ResultSet.Add(glBatch);
                wrapper.IsSuccess = true;
                return wrapper;
            }

            int newLNum = 0;
            int newJournalNo = glBatch.JnlDetails.Count;
            decimal balanceValue = 0;
            GLJnlDetailModel glJnlDetail = new GLJnlDetailModel();
            for (int i = 0; i < glBatch.JnlDetails.Count; i++)
            {
                for (int j = 0; j < glBatch.JnlDetails[i].JnlGLDetails.Count; j++)
                {
                    wrapper = GetGlAccountMaster(glBatch.JnlDetails[i].JnlGLDetails[j].GLAccount);
                    if (wrapper.IsSuccess == false)
                    {
                        return wrapper;
                    }
                    glAccountMaster = wrapper.ResultSet[0] as GlAccountMasterModel;

                    if (glAccountMaster.AccountType == "EX" || glAccountMaster.AccountType == "IN")
                    {
                        newLNum += 1;

                        GLJnlGLDetailModel glJnlGlDetail = new GLJnlGLDetailModel();
                        glJnlGlDetail.GLAccount = glAccountMaster.GlAccount;
                        glJnlGlDetail.JnlDB = glBatch.JnlDetails[i].JnlGLDetails[j].JnlCR;
                        glJnlGlDetail.JnlCR = glBatch.JnlDetails[i].JnlGLDetails[j].JnlDB;
                        glJnlGlDetail.SourceDb = glBatch.JnlDetails[i].JnlGLDetails[j].SourceCr;
                        glJnlGlDetail.SourceCr = glBatch.JnlDetails[i].JnlGLDetails[j].SourceDb;
                        glJnlGlDetail.SourceCurrency = glBatch.JnlDetails[i].JnlGLDetails[j].SourceCurrency;
                        glJnlGlDetail.ExchRate = glBatch.JnlDetails[i].JnlGLDetails[j].ExchRate;
                        glJnlGlDetail.Comment = "Balancing entry for expense accounts";
                        glJnlGlDetail.SourceTransDate = DateTime.Now;
                        glJnlDetail.JnlGLDetails.Add(glJnlGlDetail);

                        balanceValue = balanceValue + Convert.ToDecimal(glBatch.JnlDetails[i].JnlGLDetails[j].JnlDB) - Convert.ToDecimal(glBatch.JnlDetails[i].JnlGLDetails[j].JnlCR);
                    }
                    else
                    {
                        wrapper = UpdateBalance(glBatch.JnlDetails[i].FinYear + 1,
                                                0,
                                                glBatch.JnlDetails[i].JnlGLDetails[j].GLAccount,
                                                Convert.ToDecimal(glBatch.JnlDetails[i].JnlGLDetails[j].JnlDB) - Convert.ToDecimal(glBatch.JnlDetails[i].JnlGLDetails[j].JnlCR),
                                                glBatch.JnlDetails[i].JnlGLDetails[j].Quantity);
                    }
                }
            }

            if (glJnlDetail.JnlGLDetails.Count > 0)
            {
                glBatch.JnlDetails.Add(glJnlDetail);
            }

            if (newLNum > 0)
            {
                glBatch.JnlDetails[newJournalNo].JnlGLDetails[newLNum].GLAccount = balanceAccount;
                if (balanceValue > 0)
                {
                    glBatch.JnlDetails[newJournalNo].JnlGLDetails[newLNum].JnlDB = Convert.ToDouble(balanceValue);
                }
                else
                {
                    glBatch.JnlDetails[newJournalNo].JnlGLDetails[newLNum].JnlCR = -1 * Convert.ToDouble(balanceValue);
                }

                glBatch.JnlDetails[newJournalNo].JnlGLDetails[newLNum].Comment = "Balancing entry for expense accounts";
                glBatch.JnlDetails[newJournalNo].JnlGLDetails[newLNum].SourceTransDate = DateTime.Now;
                glBatch.JnlDetails[newJournalNo].EntryNo = newJournalNo;
                glBatch.JnlDetails[newJournalNo].JnlDate = DateTime.Today;
                glBatch.JnlDetails[newJournalNo].FinPeriod = 13;
                glBatch.JnlDetails[newJournalNo].FinYear = glBatch.JnlDetails[0].FinYear;

                wrapper = UpdateBalance(glBatch.JnlDetails[newJournalNo].FinYear + 1, 0, balanceAccount, balanceValue, 0);
                if (wrapper.IsSuccess == false)
                {
                    return wrapper;
                }

                wrapper = SaveGlBatch(glBatch);
                if (wrapper.IsSuccess == false)
                {
                    return wrapper;
                }
                else
                {
                    wrapper.ResultSet.Add(glBatch);
                    return wrapper;
                }

            }
            else
            {
                wrapper.ResultSet.Add(glBatch);
                wrapper.IsSuccess = true;
                return wrapper;
            }
        }

        public TransactionWrapper UpdateBalance(int finYear, int finPeriod, string glAccount, decimal amount, double quantity)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            GlAccountBalanceModel glAccountBalance = new GlAccountBalanceModel();

            wrapper = GetGlAccountBalance(finYear, glAccount);
            if (wrapper.IsSuccess == false)
            {
                return wrapper;
            }

            if (wrapper.ResultSet.Count == 0)
            {
                glAccountBalance.FinYear = finYear;
                glAccountBalance.GlAccount = glAccount;
                wrapper = InsertGlAccountBalanceWithDefaults(glAccountBalance.FinYear, glAccountBalance.GlAccount, 0);
                if (wrapper.IsSuccess == false)
                {
                    return wrapper;
                }
            }
            else
            {
                glAccountBalance = wrapper.ResultSet[0] as GlAccountBalanceModel;
            }

            switch (finPeriod)
            {
                case 0:
                    glAccountBalance.OpenBalance = Math.Round(glAccountBalance.OpenBalance + amount, 2, MidpointRounding.AwayFromZero);
                    break;
                case 1:
                    glAccountBalance.Period1 = Math.Round(glAccountBalance.Period1 + amount, 2, MidpointRounding.AwayFromZero);
                    glAccountBalance.QuantityPeriod1 += quantity;
                    break;
                case 2:
                    glAccountBalance.Period2 = Math.Round(glAccountBalance.Period2 + amount, 2, MidpointRounding.AwayFromZero);
                    glAccountBalance.QuantityPeriod2 += quantity;
                    break;
                case 3:
                    glAccountBalance.Period3 = Math.Round(glAccountBalance.Period3 + amount, 2, MidpointRounding.AwayFromZero);
                    glAccountBalance.QuantityPeriod3 += quantity;
                    break;
                case 4:
                    glAccountBalance.Period4 = Math.Round(glAccountBalance.Period4 + amount, 2, MidpointRounding.AwayFromZero);
                    glAccountBalance.QuantityPeriod4 += quantity;
                    break;
                case 5:
                    glAccountBalance.Period5 = Math.Round(glAccountBalance.Period5 + amount, 2, MidpointRounding.AwayFromZero);
                    glAccountBalance.QuantityPeriod5 += quantity;
                    break;
                case 6:
                    glAccountBalance.Period6 = Math.Round(glAccountBalance.Period6 + amount, 2, MidpointRounding.AwayFromZero);
                    glAccountBalance.QuantityPeriod6 += quantity;
                    break;
                case 7:
                    glAccountBalance.Period7 = Math.Round(glAccountBalance.Period7 + amount, 2, MidpointRounding.AwayFromZero);
                    glAccountBalance.QuantityPeriod7 += quantity;
                    break;
                case 8:
                    glAccountBalance.Period8 = Math.Round(glAccountBalance.Period8 + amount, 2, MidpointRounding.AwayFromZero);
                    glAccountBalance.QuantityPeriod8 += quantity;
                    break;
                case 9:
                    glAccountBalance.Period9 = Math.Round(glAccountBalance.Period9 + amount, 2, MidpointRounding.AwayFromZero);
                    glAccountBalance.QuantityPeriod9 += quantity;
                    break;
                case 10:
                    glAccountBalance.Period10 = Math.Round(glAccountBalance.Period10 + amount, 2, MidpointRounding.AwayFromZero);
                    glAccountBalance.QuantityPeriod10 += quantity;
                    break;
                case 11:
                    glAccountBalance.Period11 = Math.Round(glAccountBalance.Period11 + amount, 2, MidpointRounding.AwayFromZero);
                    glAccountBalance.QuantityPeriod11 += quantity;
                    break;
                case 12:
                    glAccountBalance.Period12 = Math.Round(glAccountBalance.Period12 + amount, 2, MidpointRounding.AwayFromZero);
                    glAccountBalance.QuantityPeriod12 += quantity;
                    break;
                case 13:
                    glAccountBalance.Period13 = Math.Round(glAccountBalance.Period13 + amount, 2, MidpointRounding.AwayFromZero);
                    glAccountBalance.QuantityPeriod13 += quantity;
                    break;
                default:
                    break;
            }

            wrapper = UpdateGlAccountBalance(glAccountBalance);
            if (wrapper.IsSuccess == false)
            {
                return wrapper;
            }

            if (finPeriod == 13)
            {
                decimal openBalance = Math.Round(glAccountBalance.OpenBalance +
                                                glAccountBalance.Period1 +
                                                glAccountBalance.Period2 +
                                                glAccountBalance.Period3 +
                                                glAccountBalance.Period4 +
                                                glAccountBalance.Period5 +
                                                glAccountBalance.Period6 +
                                                glAccountBalance.Period7 +
                                                glAccountBalance.Period8 +
                                                glAccountBalance.Period9 +
                                                glAccountBalance.Period10 +
                                                glAccountBalance.Period11 +
                                                glAccountBalance.Period12 +
                                                glAccountBalance.Period13 +
                                                glAccountBalance.Period14 +
                                                glAccountBalance.Period15, 2, MidpointRounding.AwayFromZero);

                wrapper = GetGlAccountBalance(finYear + 1, glAccount); // check if balances for next year exist
                if (wrapper.IsSuccess == false)
                {
                    return wrapper;
                }

                if (wrapper.ResultSet.Count == 0)
                {
                    wrapper = InsertGlAccountBalanceWithDefaults(finYear + 1, glAccount, openBalance); // create new year entry
                }
                else
                {
                    wrapper = UpdateGlAccountOpenBalance(openBalance, glAccount, finYear + 1);
                }

            }

            return wrapper;

        }

        public TransactionWrapper UpdateGlAccountOpenBalance(decimal openBalance, string glAccount, int finYear)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string updateString = GLAccountSQL.ResourceManager.GetString("UpdateGlAccountOpenBalance");
                    using (OdbcCommand command = new OdbcCommand(updateString, connection))
                    {
                        command.Parameters.Add("@OpenBalance", OdbcType.Decimal).Value = openBalance;
                        command.Parameters.Add("@GlAccount", OdbcType.VarChar).Value = glAccount;
                        command.Parameters.Add("@FinYear", OdbcType.Int).Value = finYear;

                        int rowsAffected = command.ExecuteNonQuery();
                        wrapper.IsSuccess = true;
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("UpdateGlAccountOpenBalance : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper UpdateGlAccountBalance(GlAccountBalanceModel glAccountBalance)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string updateString = GLAccountSQL.ResourceManager.GetString("UpdateGlAccountBalance");
                    using (OdbcCommand command = new OdbcCommand(updateString, connection))
                    {
                        command.Parameters.Add("@FinYear", OdbcType.Int).Value = glAccountBalance.FinYear;
                        command.Parameters.Add("@GlAccount", OdbcType.VarChar).Value = glAccountBalance.GlAccount;
                        command.Parameters.Add("@OpenBalance", OdbcType.Double).Value = glAccountBalance.OpenBalance;
                        command.Parameters.Add("@Period1", OdbcType.Double).Value = glAccountBalance.Period1;
                        command.Parameters.Add("@Period2", OdbcType.Double).Value = glAccountBalance.Period2;
                        command.Parameters.Add("@Period3", OdbcType.Double).Value = glAccountBalance.Period3;
                        command.Parameters.Add("@Period4", OdbcType.Double).Value = glAccountBalance.Period4;
                        command.Parameters.Add("@Period5", OdbcType.Double).Value = glAccountBalance.Period5;
                        command.Parameters.Add("@Period6", OdbcType.Double).Value = glAccountBalance.Period6;
                        command.Parameters.Add("@Period7", OdbcType.Double).Value = glAccountBalance.Period7;
                        command.Parameters.Add("@Period8", OdbcType.Double).Value = glAccountBalance.Period8;
                        command.Parameters.Add("@Period9", OdbcType.Double).Value = glAccountBalance.Period9;
                        command.Parameters.Add("@Period10", OdbcType.Double).Value = glAccountBalance.Period10;
                        command.Parameters.Add("@Period11", OdbcType.Double).Value = glAccountBalance.Period11;
                        command.Parameters.Add("@Period12", OdbcType.Double).Value = glAccountBalance.Period12;
                        command.Parameters.Add("@Period13", OdbcType.Double).Value = glAccountBalance.Period13;
                        command.Parameters.Add("@Period14", OdbcType.Double).Value = glAccountBalance.Period14;
                        command.Parameters.Add("@Period15", OdbcType.Double).Value = glAccountBalance.Period15;
                        command.Parameters.Add("@QtyPeriod1", OdbcType.Double).Value = glAccountBalance.QuantityPeriod1;
                        command.Parameters.Add("@QtyPeriod2", OdbcType.Double).Value = glAccountBalance.QuantityPeriod2;
                        command.Parameters.Add("@QtyPeriod3", OdbcType.Double).Value = glAccountBalance.QuantityPeriod3;
                        command.Parameters.Add("@QtyPeriod4", OdbcType.Double).Value = glAccountBalance.QuantityPeriod4;
                        command.Parameters.Add("@QtyPeriod5", OdbcType.Double).Value = glAccountBalance.QuantityPeriod5;
                        command.Parameters.Add("@QtyPeriod6", OdbcType.Double).Value = glAccountBalance.QuantityPeriod6;
                        command.Parameters.Add("@QtyPeriod7", OdbcType.Double).Value = glAccountBalance.QuantityPeriod7;
                        command.Parameters.Add("@QtyPeriod8", OdbcType.Double).Value = glAccountBalance.QuantityPeriod8;
                        command.Parameters.Add("@QtyPeriod9", OdbcType.Double).Value = glAccountBalance.QuantityPeriod9;
                        command.Parameters.Add("@QtyPeriod10", OdbcType.Double).Value = glAccountBalance.QuantityPeriod10;
                        command.Parameters.Add("@QtyPeriod11", OdbcType.Double).Value = glAccountBalance.QuantityPeriod11;
                        command.Parameters.Add("@QtyPeriod12", OdbcType.Double).Value = glAccountBalance.QuantityPeriod12;
                        command.Parameters.Add("@QtyPeriod13", OdbcType.Double).Value = glAccountBalance.QuantityPeriod13;
                        command.Parameters.Add("@QtyPeriod14", OdbcType.Double).Value = glAccountBalance.QuantityPeriod14;
                        command.Parameters.Add("@QtyPeriod15", OdbcType.Double).Value = glAccountBalance.QuantityPeriod15;
                        command.Parameters.Add("@VersCtrlNo", OdbcType.Int).Value = glAccountBalance.VersionControlNo;
                        command.Parameters.Add("@FinYear2", OdbcType.Int).Value = glAccountBalance.FinYear;
                        command.Parameters.Add("@GlAccount2", OdbcType.VarChar).Value = glAccountBalance.GlAccount;

                        int rowsAffected = command.ExecuteNonQuery();
                        wrapper.IsSuccess = true;
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("UpdateGlAccountBalance : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper InsertGlAccountBalanceWithDefaults(int finYear, string glAccount, decimal openBalance)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string insertString = GLAccountSQL.ResourceManager.GetString("InsertGlAccountBalanceWithDefaults");
                    using (OdbcCommand command = new OdbcCommand(insertString, connection))
                    {
                        command.Parameters.Add("@FinYear", OdbcType.Int).Value = finYear;
                        command.Parameters.Add("@GlAccount", OdbcType.VarChar).Value = glAccount;
                        command.Parameters.Add("@OpenBalance", OdbcType.Decimal).Value = openBalance;

                        int rowsAffected = command.ExecuteNonQuery();
                        wrapper.IsSuccess = true;
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("InsertGLAccountBalance : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper GetGlAccountBalance(int finYear, string glAccount)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = GLAccountSQL.ResourceManager.GetString("GetGlAccountBalance");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@FinYear", OdbcType.Int).Value = finYear;
                        command.Parameters.Add("@GlAccount", OdbcType.VarChar).Value = glAccount;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    dynamic dReader = new DynamicDataReader(reader);
                                    GlAccountBalanceModel glAccountBalance = new GlAccountBalanceModel
                                    {
                                        FinYear = dReader.fin_year,
                                        GlAccount = dReader.gl_account,
                                        OpenBalance = dReader.open_balance,
                                        Period1 = dReader.period_1,
                                        Period2 = dReader.period_2,
                                        Period3 = dReader.period_3,
                                        Period4 = dReader.period_4,
                                        Period5 = dReader.period_5,
                                        Period6 = dReader.period_6,
                                        Period7 = dReader.period_7,
                                        Period8 = dReader.period_8,
                                        Period9 = dReader.period_9,
                                        Period10 = dReader.period_10,
                                        Period11 = dReader.period_11,
                                        Period12 = dReader.period_12,
                                        Period13 = dReader.period_13,
                                        Period14 = dReader.period_14,
                                        Period15 = dReader.period_15,
                                        QuantityPeriod1 = dReader.qtyperiod_1,
                                        QuantityPeriod2 = dReader.qtyperiod_2,
                                        QuantityPeriod3 = dReader.qtyperiod_3,
                                        QuantityPeriod4 = dReader.qtyperiod_4,
                                        QuantityPeriod5 = dReader.qtyperiod_5,
                                        QuantityPeriod6 = dReader.qtyperiod_6,
                                        QuantityPeriod7 = dReader.qtyperiod_7,
                                        QuantityPeriod8 = dReader.qtyperiod_8,
                                        QuantityPeriod9 = dReader.qtyperiod_9,
                                        QuantityPeriod10 = dReader.qtyperiod_10,
                                        QuantityPeriod11 = dReader.qtyperiod_11,
                                        QuantityPeriod12 = dReader.qtyperiod_12,
                                        QuantityPeriod13 = dReader.qtyperiod_13,
                                        QuantityPeriod14 = dReader.qtyperiod_14,
                                        QuantityPeriod15 = dReader.qtyperiod_15,
                                        VersionControlNo = dReader.version_ctrl_no
                                    };
                                    wrapper.ResultSet.Add(glAccountBalance);
                                }
                            }
                            else
                            {
                                wrapper.IsSuccess = true;
                                return wrapper;
                            }
                        }
                    }

                    wrapper.IsSuccess = true;
                    return wrapper;

                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetGlAccountBalance : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper SaveGlBatch(GLBatchHeaderModel glBatch)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            List<string> messages = new List<string>();
            try
            {
                bool isSaved = false;

                string intercompanyaction = "Y";

                // see IF the record exists
                wrapper = GetGLBatchHeaderByBatchNo(glBatch.BatchNo);
                if (wrapper.IsSuccess == false)
                {
                    return wrapper;
                }

                // no errors, found record
                if (wrapper.ResultSet.Count > 0)
                {
                    GLBatchHeaderModel batchHeader = wrapper.ResultSet[0] as GLBatchHeaderModel;
                    isSaved = true;
                    // cant change the source if the journal has been imported from a different system. 
                    if (IsJournalImported(glBatch))
                    {
                        if (batchHeader.Source != glBatch.Source)
                        {
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add("SaveGlBatch : You Cannot change the source of an imported journal.");
                            return wrapper;
                        }
                    }

                    // cant save changes to Posted or Deleted records 
                    if (batchHeader.Status == "P")
                    {
                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add("SaveGlBatch : Cannot make changes to a Batch that has been Posted.");
                        return wrapper;
                    }
                    else if (batchHeader.Status == "D")
                    {
                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add("SaveGlBatch : Cannot make changes to a Batch that has been deleted.");
                        return wrapper;
                    }
                    else if (batchHeader.Status == "C")// IF record has been Closed then can can only save valid data
                    {
                        wrapper = ValidateBatch(batchHeader);
                        if (wrapper.IsSuccess == false)
                        {
                            return wrapper;
                        }
                    }
                }
                else
                {
                    isSaved = false;
                }

                //Validate fin_period
                foreach (GLJnlDetailModel jnlDetRow in glBatch.JnlDetails)
                {
                    if (jnlDetRow.FinPeriod == 0)
                    {
                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add("SaveJnlGLTransactions : Financial Period cannot be zero.");
                        return wrapper;
                    }
                }

                bool isInterCompany = false;
                //validate IsInterCompany
                ValidateInterComp(ref isInterCompany, glBatch);

                if (isInterCompany && intercompanyaction == "Y")
                {
                    messages.Add("Make Contra start");
                    wrapper = MakeContra(glBatch);
                    if (wrapper.IsSuccess == false)
                    {
                        return wrapper;
                    }

                    glBatch = wrapper.ResultSet[0] as GLBatchHeaderModel;
                    messages.Add("Make Contra end");
                }

                //UPDATE the total amount
                messages.Add("Update amount start");
                foreach (GLJnlDetailModel jnlDetRow in glBatch.JnlDetails)
                {
                    foreach (GLJnlGLDetailModel jnlGLDetRow in jnlDetRow.JnlGLDetails)
                    {
                        if (!string.IsNullOrEmpty(jnlGLDetRow.SourceType) && !string.IsNullOrEmpty(jnlGLDetRow.SourceDoc))
                        {
                            glBatch.JournalAmount = glBatch.JournalAmount +
                                jnlGLDetRow.JnlDB;
                        }
                    }
                }
                messages.Add("Update amount end");
                // IF saving FROM a standing transaction THEN save it with status 'new'
                messages.Add("Check status start");
                if (String.IsNullOrEmpty(glBatch.Status)) // first check if status is null to avoid null exception
                {
                    glBatch.Status = "N";
                }
                else if (glBatch.Status == "S")
                {
                    glBatch.Status = "N";
                }
                messages.Add("check status end");

                // save the header 
                if (isSaved)
                {
                    glBatch.ModifiedDate = DateTime.Now;
                    wrapper = UpdateHeaderByBatchNo(glBatch);
                }
                else
                {
                    if (glBatch.CreatedDate == default(DateTime))
                    {
                        glBatch.CreatedDate = DateTime.Now;
                    }
                    if (glBatch.ModifiedDate == default(DateTime))
                    {
                        glBatch.ModifiedDate = DateTime.Now;
                    }
                    glBatch.CreatedBy = glBatch.Originator;
                    wrapper = InsertBatchHeader(glBatch);
                }

                // the return status will return a zero value If the db query suceeded 
                // in other circumstance it will return the ERRORCODE
                if (wrapper.IsSuccess == false)
                {
                    return wrapper;
                }

                //delete all the details 
                wrapper = DeleteBatchDetail(glBatch.BatchNo);
                if (wrapper.IsSuccess == false)
                {
                    return wrapper;
                }

                for (int i = 0; i < glBatch.JnlDetails.Count; i++)
                {
                    string journalDate = DateTime.Parse(glBatch.JnlDetails[i].JnlDate.ToShortDateString(), new CultureInfo("en-AU")).ToString(DateFormats.ddMMyy);

                    wrapper = ValidateAccounts(glBatch.JnlDetails[i]);
                    if (wrapper.IsSuccess == false)
                    {
                        return wrapper;
                    }

                    for (int j = 0; j < glBatch.JnlDetails[i].JnlGLDetails.Count; j++)
                    {
                        glBatch.JnlDetails[i].JnlGLDetails[j].SortNo = j + 1;
                        glBatch.JnlDetails[i].JnlGLDetails[j].BatchNo = glBatch.BatchNo;
                        glBatch.JnlDetails[i].JnlGLDetails[j].EntryNo = glBatch.JnlDetails[i].EntryNo;
                        glBatch.JnlDetails[i].JnlGLDetails[j].FinYear = glBatch.JnlDetails[i].FinYear;
                        glBatch.JnlDetails[i].JnlGLDetails[j].FinPeriod = glBatch.JnlDetails[i].FinPeriod;
                        glBatch.JnlDetails[i].JnlGLDetails[j].JnlDate = journalDate;
                        wrapper = InsertGlBatchDetail(glBatch.JnlDetails[i].JnlGLDetails[j]);
                    }
                }
                wrapper.ResultSet.Clear();
                wrapper.ResultSet.Add(glBatch);
                return wrapper;
            }
            catch (Exception e)
            {
                wrapper.ResultSet.Add(glBatch);
                wrapper.IsSuccess = false;
                wrapper.Messages = messages;
                wrapper.Messages.Add(e.ToString());
                return wrapper;
            }
        }

        public TransactionWrapper UpdateGlBatchHeaderStatus(string originator, string status, int batchNo)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string updateString = GLAccountSQL.ResourceManager.GetString("UpdateGlBatchHeaderStatus");
                    using (OdbcCommand command = new OdbcCommand(updateString, connection))
                    {
                        command.Parameters.Add("@ModifiedDate", OdbcType.VarChar).Value = DateTime.Now.ToString(DateFormats.MMddyy);
                        command.Parameters.Add("@Originator", OdbcType.VarChar).Value = originator;
                        command.Parameters.Add("@Status", OdbcType.VarChar).Value = status;
                        command.Parameters.Add("@BatchNo", OdbcType.Int).Value = batchNo;

                        int rowsAffected = command.ExecuteNonQuery();
                        wrapper.IsSuccess = true;
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("UpdateGlBatchHeaderStatus : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper InsertGlBatchDetail(GLJnlGLDetailModel glBatchDetail)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            if (glBatchDetail.SourceCurrency == "" || glBatchDetail.ExchRate == 0)
            {
                glBatchDetail.ExchRate = 1;
            }

            // not sure if this block ever gets hit via the gun but included anyway
            if (glBatchDetail.SourceDb != 0 || glBatchDetail.SourceCr != 0 || glBatchDetail.SourceAmount != 0)
            {
                if (glBatchDetail.SourceDb != 0)
                {
                    glBatchDetail.SourceAmount = glBatchDetail.SourceDb;
                }
                else if (glBatchDetail.SourceCr != 0)
                {
                    glBatchDetail.SourceAmount = -1 * glBatchDetail.SourceCr;
                }

                if (glBatchDetail.JnlAmount >= (glBatchDetail.SourceAmount * glBatchDetail.ExchRate) + 0.01 ||
                glBatchDetail.JnlAmount <= (glBatchDetail.SourceAmount * glBatchDetail.ExchRate) - 0.01)
                {
                    glBatchDetail.JnlAmount = glBatchDetail.SourceAmount * glBatchDetail.ExchRate;
                }
            }
            else // this seems to be the block always executed, I think
            {
                if (glBatchDetail.JnlDB != 0)
                {
                    glBatchDetail.JnlAmount = glBatchDetail.JnlDB;
                }
                else if (glBatchDetail.JnlCR != 0)
                {
                    glBatchDetail.JnlAmount = -1 * glBatchDetail.JnlCR;
                }

                if (glBatchDetail.JnlAmount >= (glBatchDetail.SourceAmount * glBatchDetail.ExchRate) + 0.01 ||
                glBatchDetail.JnlAmount <= (glBatchDetail.SourceAmount * glBatchDetail.ExchRate) - 0.01)
                {
                    glBatchDetail.SourceAmount = glBatchDetail.JnlAmount / glBatchDetail.ExchRate;
                }
            }

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string insertString = GLAccountSQL.ResourceManager.GetString("InsertGlBatchDetail");
                    using (OdbcCommand command = new OdbcCommand(insertString, connection))
                    {
                        command.Parameters.Add("@BatchNo", OdbcType.Int).Value = glBatchDetail.BatchNo;
                        command.Parameters.Add("@Comment", OdbcType.VarChar).Value = glBatchDetail.Comment;
                        command.Parameters.Add("@Description", OdbcType.VarChar).Value = "";
                        command.Parameters.Add("@EntryNo", OdbcType.Int).Value = glBatchDetail.EntryNo;
                        command.Parameters.Add("@ExchRate", OdbcType.Double).Value = glBatchDetail.ExchRate;
                        command.Parameters.Add("@FinPeriod", OdbcType.Int).Value = glBatchDetail.FinPeriod;
                        command.Parameters.Add("@FinYear", OdbcType.Int).Value = glBatchDetail.FinYear;
                        command.Parameters.Add("@GlAccount", OdbcType.VarChar).Value = glBatchDetail.GLAccount;
                        command.Parameters.Add("@JnlAmount", OdbcType.Double).Value = glBatchDetail.JnlAmount;
                        command.Parameters.Add("@JnlDate", OdbcType.VarChar).Value = DateTime.Parse(glBatchDetail.JnlDate).ToString(DateFormats.MMddyy);
                        command.Parameters.Add("@Quantity", OdbcType.Double).Value = glBatchDetail.Quantity;
                        command.Parameters.Add("@Reference", OdbcType.VarChar).Value = glBatchDetail.Reference;
                        command.Parameters.Add("@SourceAmount", OdbcType.Double).Value = glBatchDetail.SourceAmount;
                        command.Parameters.Add("@SourceCur", OdbcType.VarChar).Value = glBatchDetail.SourceCurrency;
                        command.Parameters.Add("@SourceDoc", OdbcType.VarChar).Value = glBatchDetail.SourceDoc;
                        command.Parameters.Add("@SourceType", OdbcType.VarChar).Value = glBatchDetail.SourceType;
                        command.Parameters.Add("@SortNo", OdbcType.Int).Value = glBatchDetail.SortNo;
                        command.Parameters.Add("@SubCode", OdbcType.VarChar).Value = "";
                        command.Parameters.Add("@Uom", OdbcType.VarChar).Value = "";
                        command.Parameters.Add("@SourceTransDate", OdbcType.VarChar).Value = glBatchDetail.SourceTransDate.ToString(DateFormats.MMddyy);

                        int rowsAffected = command.ExecuteNonQuery();

                        wrapper.IsSuccess = true;
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("InsertGlBatchDetail : " + e.Message);
                    return wrapper;
                }
            }


        }

        public TransactionWrapper ValidateAccounts(GLJnlDetailModel glJournalDetail)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            for (int i = 0; i < glJournalDetail.JnlGLDetails.Count; i++)
            {
                using (OdbcConnection connection = new OdbcConnection(connectionString))
                {
                    try
                    {
                        connection.Open();
                        string queryString = GLAccountSQL.ResourceManager.GetString("GetAccountMasterStatus");
                        using (OdbcCommand command = new OdbcCommand(queryString, connection))
                        {
                            command.Parameters.Add("@GlAccount", OdbcType.VarChar).Value = glJournalDetail.JnlGLDetails[i].GLAccount;

                            using (OdbcDataReader reader = command.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    string status = reader.GetString(0);
                                    if (status == "I")
                                    {
                                        wrapper.IsSuccess = false;
                                        wrapper.Messages.Add(glJournalDetail.JnlGLDetails[i].GLAccount + " is inactive. You will not be able to post this batch unless account status is changed");
                                        return wrapper;
                                    }
                                }
                                else
                                {
                                    wrapper.IsSuccess = false;
                                    wrapper.Messages.Add(glJournalDetail.JnlGLDetails[i].GLAccount + " is an invalid account number");
                                    return wrapper;
                                }
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add("ValidateAccounts : " + e.Message);
                        return wrapper;
                    }
                }
            }

            wrapper.IsSuccess = true;
            return wrapper;
        }

        public TransactionWrapper DeleteBatchDetail(int batchNo)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string insertString = GLAccountSQL.ResourceManager.GetString("DeleteBatchDetail");

                    using (OdbcCommand command = new OdbcCommand(insertString, connection))
                    {
                        command.Parameters.Add("@BatchNo", OdbcType.Int).Value = batchNo;

                        int rowsAffected = command.ExecuteNonQuery();
                        wrapper.IsSuccess = true;
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("DeleteBatchDetail : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper UpdateHeaderByBatchNo(GLBatchHeaderModel glBatchHeader)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string insertString = GLAccountSQL.ResourceManager.GetString("UpdateHeaderByBatchNo");

                    using (OdbcCommand command = new OdbcCommand(insertString, connection))
                    {
                        command.Parameters.Add("@BatchNo", OdbcType.Int).Value = glBatchHeader.BatchNo;
                        command.Parameters.Add("@Description", OdbcType.VarChar).Value = glBatchHeader.Description;
                        command.Parameters.Add("@Exported", OdbcType.Int).Value = 0;
                        command.Parameters.Add("@JnlAmount", OdbcType.Double).Value = glBatchHeader.JournalAmount;
                        command.Parameters.Add("@CreatedDate", OdbcType.VarChar).Value = "";
                        command.Parameters.Add("@ModifiedDate", OdbcType.VarChar).Value = DateTime.Now.ToString(DateFormats.MMddyy);
                        command.Parameters.Add("@Originator", OdbcType.VarChar).Value = glBatchHeader.Originator;
                        command.Parameters.Add("@Source", OdbcType.VarChar).Value = glBatchHeader.Source;
                        command.Parameters.Add("@Status", OdbcType.VarChar).Value = glBatchHeader.Status;
                        command.Parameters.Add("@BatchNo2", OdbcType.Int).Value = glBatchHeader.BatchNo;

                        int rowsAffected = command.ExecuteNonQuery();
                        wrapper.IsSuccess = true;
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("UpdateHeaderByBatchNo : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper InsertBatchHeader(GLBatchHeaderModel glBatchHeader)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string insertString = GLAccountSQL.ResourceManager.GetString("InsertBatchHeader");

                    using (OdbcCommand command = new OdbcCommand(insertString, connection))
                    {
                        command.Parameters.Add("@BatchNo", OdbcType.Int).Value = glBatchHeader.BatchNo;
                        command.Parameters.Add("@Description", OdbcType.VarChar).Value = glBatchHeader.Description;
                        command.Parameters.Add("@Exported", OdbcType.Int).Value = 0;
                        command.Parameters.Add("@JnlAmount", OdbcType.Double).Value = glBatchHeader.JournalAmount;
                        command.Parameters.Add("@CreatedDate", OdbcType.VarChar).Value = DateTime.Now.ToString(DateFormats.ddmmyyyywithouttime);
                        command.Parameters.Add("@ModifiedDate", OdbcType.VarChar).Value = DateTime.Now.ToString(DateFormats.ddmmyyyywithouttime);
                        command.Parameters.Add("@Originator", OdbcType.VarChar).Value = glBatchHeader.Originator;
                        command.Parameters.Add("@Source", OdbcType.VarChar).Value = glBatchHeader.Source;
                        command.Parameters.Add("@Status", OdbcType.VarChar).Value = glBatchHeader.Status;
                        command.Parameters.Add("@CreatedBy", OdbcType.VarChar).Value = glBatchHeader.Originator;

                        int rowsAffected = command.ExecuteNonQuery();
                        wrapper.IsSuccess = true;
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("InsertBatchHeader : " + e.Message);
                    return wrapper;
                }
            }
        }

        public bool IsJournalImported(GLBatchHeaderModel batchHeader)
        {
            foreach (GLJnlDetailModel jnlDetRow in batchHeader.JnlDetails)
            {
                foreach (GLJnlGLDetailModel jnlGLDetRow in jnlDetRow.JnlGLDetails)
                {
                    if (!string.IsNullOrEmpty(jnlGLDetRow.SourceType) && !string.IsNullOrEmpty(jnlGLDetRow.SourceDoc))
                    {
                        return true;
                    }
                }
            }

            return false;
        }

        public TransactionWrapper ValidateInterComp(ref bool isInterCompany, GLBatchHeaderModel batchHeader)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            List<string> comps = new List<string>();
            List<double> compTots = new List<double>();

            for (int i = 0; i < batchHeader.JnlDetails.Count; i++)
            {
                if (!isInterCompany)
                {
                    comps.Clear();
                    compTots.Clear();
                    for (int j = 0; j < batchHeader.JnlDetails[i].JnlGLDetails.Count; j++)
                    {
                        bool isFound = false;
                        string glAccount = batchHeader.JnlDetails[i].JnlGLDetails[j].GLAccount;

                        for (int k = 0; k < comps.Count; k++)
                        {
                            if (comps[k].Equals(glAccount.Substring(0, comps[k].Length - 1)))
                            {
                                compTots[k] += batchHeader.JnlDetails[i].JnlGLDetails[j].JnlDB;
                                compTots[k] -= batchHeader.JnlDetails[i].JnlGLDetails[j].JnlCR;

                                isFound = true;
                            }
                        }

                        if (!isFound)
                        {
                            wrapper = GetGlAccountMaster(glAccount);
                            if (wrapper.IsSuccess == false)
                            {
                                return wrapper;
                            }
                            else if (wrapper.ResultSet.Count == 0)
                            {
                                wrapper.IsSuccess = false;
                                wrapper.Messages.Add("ValidateIntercomp : Could not find account master for GL Account " + glAccount);
                                return wrapper;
                            }

                            GlAccountMasterModel glAccountMaster = wrapper.ResultSet[0] as GlAccountMasterModel;
                            comps.Add(glAccountMaster.CompanyCode);
                            compTots.Add(0);

                            compTots[compTots.Count - 1] += batchHeader.JnlDetails[i].JnlGLDetails[j].JnlDB;
                            compTots[compTots.Count - 1] -= batchHeader.JnlDetails[i].JnlGLDetails[j].JnlCR;
                        }
                    }

                    for (int k = 0; k < comps.Count; k++)
                    {
                        if (Math.Abs(compTots[k]) > 0.001)
                        {
                            isInterCompany = true;
                        }
                    }
                }
            }

            wrapper.IsSuccess = true;
            return wrapper;
        }

        public TransactionWrapper MakeContra(GLBatchHeaderModel batchHeader)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            List<GlIntCompBal> comps = new List<GlIntCompBal>();
            List<CompanySegmentModel> segments = new List<CompanySegmentModel>();
            int companySegs = 0;
            string company = "";

            for (int i = 0; i < batchHeader.JnlDetails.Count; i++)
            {
                comps.Clear();
                for (int j = 0; j < batchHeader.JnlDetails[i].JnlGLDetails.Count; j++)
                {
                    wrapper = GetByCompanySegment(batchHeader.CompanyCode, ref companySegs);
                    if (wrapper.IsSuccess == false)
                    {
                        return wrapper;
                    }

                    segments = wrapper.ResultSet[0] as List<CompanySegmentModel>;

                    company = GetCompCode(segments, batchHeader.JnlDetails[i].JnlGLDetails[j].GLAccount, companySegs);

                    int row = 0;
                    if (comps.Count > 0)
                    {
                        row = -1;
                        for (int k = 0; k < comps.Count; k++)
                        {
                            if (comps[k].CompanyCode.Trim().Equals(company))
                            {
                                row = k;
                                break;
                            }
                        }
                        if (row < 0)
                        {
                            GlIntCompBal comp = new GlIntCompBal
                            {
                                CompanyCode = company
                            };
                            comps.Add(comp);
                            row = comps.Count - 1;
                        }
                    }
                    else
                    {
                        GlIntCompBal comp = new GlIntCompBal
                        {
                            CompanyCode = company
                        };
                        comps.Add(comp);
                        row = comps.Count - 1;
                    }

                    comps[row].Total += batchHeader.JnlDetails[i].JnlGLDetails[j].JnlDB;
                    comps[row].Total -= batchHeader.JnlDetails[i].JnlGLDetails[j].JnlCR;
                }

                comps.Sort((x, y) => x.Total.CompareTo(y.Total));

                if (!String.IsNullOrEmpty(batchHeader.CompanyCode))
                {
                    bool isFound = false;
                    for (int k = 0; k < comps.Count; k++)
                    {
                        if (comps[i].CompanyCode.Trim().Equals(batchHeader.CompanyCode))
                        {
                            isFound = true;
                            break;
                        }

                        if (!isFound)
                        {
                            batchHeader.CompanyCode = comps[0].CompanyCode;
                        }
                    }
                }
                else
                {
                    batchHeader.CompanyCode = comps[0].CompanyCode;
                }

                if (comps.Count > 0)
                {
                    for (int k = 0; k < comps.Count; k++)
                    {
                        company = comps[k].CompanyCode;

                        if (!company.Trim().Equals(batchHeader.CompanyCode.Trim()))
                        {
                            string paramName = "CB_LOAN_GL_" + company + "_" + batchHeader.CompanyCode;
                            wrapper = GetJobSettingsByParamName(paramName);

                            if (wrapper.IsSuccess == false)
                            {
                                return wrapper;
                            }

                            GLSettingsModel settings = wrapper.ResultSet[0] as GLSettingsModel;
                            if (!batchHeader.JnlDetails[i].JnlGLDetails.Exists(x => x.GLAccount == settings.ParamValChar))
                            {
                                GLJnlGLDetailModel gLJnlGLDetailModel = new GLJnlGLDetailModel
                                {
                                    GLAccount = settings.ParamValChar,
                                    EntryNo = i
                                };

                                if (comps[k].Total < 0)
                                {
                                    gLJnlGLDetailModel.JnlCR = Math.Abs(comps[k].Total);
                                }
                                else
                                {
                                    gLJnlGLDetailModel.JnlDB = Math.Abs(comps[k].Total);
                                }

                                gLJnlGLDetailModel.SourceType = "";
                                gLJnlGLDetailModel.Comment = "automatic intercompany contra entry";
                                batchHeader.JnlDetails[i].JnlGLDetails.Add(gLJnlGLDetailModel);
                            }

                            paramName = "CB_LOAN_GL_" + batchHeader.CompanyCode + "_" + company;
                            wrapper = GetJobSettingsByParamName(paramName);

                            if (wrapper.IsSuccess == false)
                            {
                                return wrapper;
                            }

                            settings = wrapper.ResultSet[0] as GLSettingsModel;
                            if (!batchHeader.JnlDetails[i].JnlGLDetails.Exists(x => x.GLAccount == settings.ParamValChar))
                            {
                                GLJnlGLDetailModel gLJnlGLDetailModel = new GLJnlGLDetailModel
                                {
                                    GLAccount = settings.ParamValChar,
                                    EntryNo = i
                                };

                                if (comps[k].Total < 0)
                                {
                                    gLJnlGLDetailModel.JnlDB = Math.Abs(comps[k].Total);
                                }
                                else
                                {
                                    gLJnlGLDetailModel.JnlCR = Math.Abs(comps[k].Total);
                                }

                                gLJnlGLDetailModel.SourceType = "";
                                gLJnlGLDetailModel.Comment = "automatic intercompany contra entry";
                                batchHeader.JnlDetails[i].JnlGLDetails.Add(gLJnlGLDetailModel);
                            }
                        }
                    }
                }
            }
            wrapper.ResultSet.Clear();
            wrapper.ResultSet.Add(batchHeader);
            wrapper.IsSuccess = true;
            return wrapper;
        }

        public string GetCompCode(List<CompanySegmentModel> segments, string glAccount, int companySegs)
        {
            int removeGl = 0;
            string company = "";

            for (int i = 0; i < companySegs; i++)
            {
                removeGl = removeGl + segments[i].SegLength.Length - 1;
            }

            if (companySegs != 0) // should never be zero as would have previously failed, here for completion sake
            {
                /*
                company = glAccount.Substring(0, removeGl + segments[companySegs].SegLength.Length); // LEFT
                company = company.Substring((company.Length - 1) - (segments[companySegs].SegLength.Length - 1)); // RIGHT */
                company = glAccount.Substring(0, 1);
            }

            return company;
        }

        public TransactionWrapper GetByCompanySegment(string companyCode, ref int companySeg)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            List<CompanySegmentModel> segmentList = new List<CompanySegmentModel>();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = GLAccountSQL.ResourceManager.GetString("GetByCompanySegmentByCompanyCode");

                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@CompanyCode", OdbcType.VarChar).Value = companyCode;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            dynamic dReader = new DynamicDataReader(reader);

                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    CompanySegmentModel segment = new CompanySegmentModel
                                    {
                                        CompanyCode = dReader.company_code,
                                        SegNo = dReader.seg_no,
                                        SegName = dReader.seg_name,
                                        SegLength = dReader.seg_length,
                                        SegUsed = dReader.seg_used
                                    };

                                    segmentList.Add(segment);
                                }
                            }
                            else
                            {
                                wrapper.IsSuccess = false;
                                wrapper.Messages.Add("GetByCompanySegment: " + "No GL segment found for company " + companyCode);
                                return wrapper;
                            }
                        }

                        for (int i = 0; i < segmentList.Count; i++)
                        {
                            if (segmentList[i].SegName.Trim().Equals("COMPANY"))
                            {
                                companySeg = i + 1;
                            }
                        }

                        if (companySeg == 0)
                        {
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add("GetByCompanySegment : Error retrieving GL segment for the company " + companyCode);
                            return wrapper;
                        }
                        else
                        {
                            wrapper.ResultSet.Add(segmentList);
                            wrapper.IsSuccess = true;
                            return wrapper;
                        }
                    }


                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetByCompanySegment: " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper GetInterCompanyApproval(int batchNo)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = GLAccountSQL.ResourceManager.GetString("GetIntercompanyApproval");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@BatchNo", OdbcType.Int).Value = batchNo;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                wrapper.IsSuccess = false;
                                wrapper.Messages.Add("This intercompany journal has not been approved by all companies");
                                return wrapper;
                            }
                            else
                            {
                                wrapper.IsSuccess = true;
                                return wrapper;
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetInterCompanyApproval : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper GetFinPeriodZeroCount(ref int count, int batchNo)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = GLAccountSQL.ResourceManager.GetString("GetFinPeriodZeroCount");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@BatchNo", OdbcType.Int).Value = batchNo;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                count = reader.GetInt32(0);
                            }
                        }
                    }

                    wrapper.IsSuccess = true;
                    return wrapper;
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetFinPeriodZeroCount : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper CheckIfCostYearAndPeriodLocked(ref int costYear, ref int costPeriod, int batchNo)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = GLAccountSQL.ResourceManager.GetString("GetCostYearAndPeriod");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@BatchNo", OdbcType.Int).Value = batchNo;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    costYear = reader.GetInt32(0);
                                    costPeriod = reader.GetInt32(1);
                                }
                            }
                            else
                            {
                                wrapper.IsSuccess = true;
                                return wrapper;
                            }
                        }
                    }

                    wrapper.IsSuccess = true;
                    return wrapper;

                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetCostYearAndPeriod : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper GetGlAccountMaster(string glAccount)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            GlAccountMasterModel glAccountMaster = new GlAccountMasterModel();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = GLAccountSQL.ResourceManager.GetString("GetGlAccountMaster");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@GLAccount", OdbcType.VarChar).Value = glAccount;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                dynamic dReader = new DynamicDataReader(reader);
                                while (reader.Read())
                                {
                                    glAccountMaster.AccountBalance = dReader.account_balance;
                                    glAccountMaster.AccountGroup = dReader.account_group;
                                    glAccountMaster.AccountType = dReader.account_type;
                                    glAccountMaster.AutoAllocate = dReader.auto_allocate;
                                    glAccountMaster.CompanyCode = dReader.company_code;
                                    glAccountMaster.Currency = dReader.currency;
                                    glAccountMaster.Description = dReader.description;
                                    glAccountMaster.GlAccount = dReader.gl_account;
                                    glAccountMaster.NormalBalance = dReader.normal_balance;
                                    glAccountMaster.PostToMethod = dReader.post_to_method;
                                    glAccountMaster.Status = dReader.status;
                                    glAccountMaster.VersionControlNo = dReader.version_ctrl_no;
                                    glAccountMaster.InterCompLoanAcc = dReader.inter_comp_loan_acc;

                                    wrapper.IsSuccess = true;
                                    wrapper.ResultSet.Add(glAccountMaster);
                                    return wrapper;
                                }
                            }
                        }

                        wrapper.IsSuccess = true;
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetGLAccountMaster : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper CheckActiveAccount(ref string status, string glAccount)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = GLAccountSQL.ResourceManager.GetString("GetAccountStatus");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@GLAccount", OdbcType.VarChar).Value = glAccount;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                status = reader.GetString(0);
                            }
                        }
                    }

                    wrapper.IsSuccess = true;
                    return wrapper;
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("CheckActiveAccount : " + e.Message);
                    return wrapper;
                }
            }

        }

        public TransactionWrapper GetEoyHistoryList(int batchNo)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            List<EoyHistoryModel> eoyList = new List<EoyHistoryModel>();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = GLAccountSQL.ResourceManager.GetString("GetEOYHistory");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@BatchNo", OdbcType.Int).Value = batchNo;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                EoyHistoryModel eoyModel = new EoyHistoryModel
                                {
                                    CompanyCode = reader.GetString(0),
                                    ProcYear = reader.GetInt32(1)
                                };
                                eoyList.Add(eoyModel);
                            }
                        }
                    }

                    wrapper.ResultSet.Add(eoyList);
                    wrapper.IsSuccess = true;
                    return wrapper;
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetEoyHistory : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper GetEoyHistoryByCompanyCodeAndYear(string companyCode, int finYear)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            EoyHistoryModel eoyHistory = new EoyHistoryModel();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = GLAccountSQL.ResourceManager.GetString("GetEoyHistoryByCodeAndYear");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@CompanyCode", OdbcType.VarChar).Value = companyCode;
                        command.Parameters.Add("@ProcYear", OdbcType.Int).Value = finYear;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                dynamic dReader = new DynamicDataReader(reader);
                                while (reader.Read())
                                {
                                    eoyHistory.CompanyCode = dReader.company_code;
                                    eoyHistory.GlBatchNo = dReader.gl_batch_no;
                                    eoyHistory.Posted = dReader.posted;
                                    eoyHistory.ProcYear = dReader.proc_year;

                                    wrapper.IsSuccess = true;
                                    wrapper.ResultSet.Add(eoyHistory);
                                    return wrapper;
                                }
                            }
                        }
                    }

                    wrapper.IsSuccess = true;
                    return wrapper;
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetEoyHistoryByCodeAndYear : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper UpdateOrInsertEoyHistory(EoyHistoryModel eoyHistory)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            int rowsAffected = 0;

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string updateString = GLAccountSQL.ResourceManager.GetString("UpdateEoyHistory");
                    using (OdbcCommand command = new OdbcCommand(updateString, connection))
                    {
                        command.Parameters.Add("@CompanyCode", OdbcType.VarChar).Value = eoyHistory.CompanyCode;
                        command.Parameters.Add("@BatchNo", OdbcType.Int).Value = eoyHistory.GlBatchNo;
                        command.Parameters.Add("@Posted", OdbcType.Int).Value = eoyHistory.Posted;
                        command.Parameters.Add("@ProcYear", OdbcType.Int).Value = eoyHistory.ProcYear;
                        command.Parameters.Add("@CompanyCode2", OdbcType.VarChar).Value = eoyHistory.CompanyCode;
                        command.Parameters.Add("@ProcYear2", OdbcType.Int).Value = eoyHistory.ProcYear;

                        rowsAffected = command.ExecuteNonQuery();
                    }

                    if (rowsAffected == 0)
                    {
                        string insertString = GLAccountSQL.ResourceManager.GetString("InsertEoyHistory");
                        using (OdbcCommand command = new OdbcCommand(insertString, connection))
                        {
                            command.Parameters.Add("@CompanyCode", OdbcType.VarChar).Value = eoyHistory.CompanyCode;
                            command.Parameters.Add("@BatchNo", OdbcType.Int).Value = eoyHistory.GlBatchNo;
                            command.Parameters.Add("@Posted", OdbcType.Int).Value = eoyHistory.Posted;
                            command.Parameters.Add("@ProcYear", OdbcType.Int).Value = eoyHistory.ProcYear;

                            rowsAffected = command.ExecuteNonQuery();
                        }
                    }

                    wrapper.IsSuccess = true;
                    return wrapper;

                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("UpdateOrInsertEoyHistory : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper ValidateBatch(GLBatchHeaderModel glBatch)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            if (glBatch.BatchNo == 0)
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("Batch Header has no batch number");
                return wrapper;
            }

            if (String.IsNullOrEmpty(glBatch.Source))
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("Batch " + glBatch.BatchNo + " has no specified source");
                return wrapper;
            }
            else
            {
                using (OdbcConnection connection = new OdbcConnection(connectionString))
                {
                    try
                    {
                        connection.Open();
                        string queryString = GLAccountSQL.ResourceManager.GetString("GetSource");
                        using (OdbcCommand command = new OdbcCommand(queryString, connection))
                        {
                            command.Parameters.Add("@Source", OdbcType.VarChar).Value = glBatch.Source;
                            using (OdbcDataReader reader = command.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    wrapper.IsSuccess = true;
                                    return wrapper;
                                }
                                else
                                {
                                    wrapper.IsSuccess = false;
                                    wrapper.Messages.Add("Source code " + glBatch.Source + " does not exist");
                                    return wrapper;
                                }
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add("ValidateBatch: " + e.Message);
                        return wrapper;
                    }
                }
            }
        }
    }
}