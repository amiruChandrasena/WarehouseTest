using Abstractions.ServiceInterfaces;
using Common;
using Models;
using System;
using System.Collections.Generic;
using System.Text;
using System.Transactions;

namespace Business
{
    public class ReturnsBussiness
    {
        private readonly IJobService _jobService;
        private readonly IRawMaterialService _rawMaterialsService;
        private readonly IPalletService _palletService;
        private readonly ICatalogService _catalogService;
        private readonly IGLAccountService _glAccountService;
        private readonly IStockService _stockService;

        public ReturnsBussiness(IJobService jobService, IRawMaterialService rawMaterialsService, 
            IPalletService palletService, ICatalogService catalogService, 
            IGLAccountService glAccountService, IStockService stockService)
        {
            _jobService = jobService;
            _rawMaterialsService = rawMaterialsService;
            _palletService = palletService;
            _catalogService = catalogService;
            _glAccountService = glAccountService;
            _stockService = stockService;
        }

        public TransactionWrapper SaveRMReturn(ReturnsModel retunrModel)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            TransactionWrapper errorWrapper = new TransactionWrapper();

            string valueCompTag = "1";
            string Setting_CB_RSTK_GL = "";
            string Setting_CB_STAJ_GL = "";
            string Setting_CB_WIP_GL = "";
            string Setting_CB_RMV_GL = "";
            string Currency = "AUD";

            using (TransactionScope scope = new TransactionScope())
            {
                try
                {
                    //--get the job detail 
                    wrapper = _jobService.GetRMJobDetailsByJobNo(retunrModel.JobNo);
                    if (wrapper.IsSuccess == false)
                    {
                        scope.Dispose();
                        return wrapper;
                    }

                    JobHeaderRMModel jobHeader = wrapper.ResultSet[0] as JobHeaderRMModel;

                    /*Get GL Settings*/
                    //--RM stock Account 
                    wrapper = _glAccountService.GetJobSettingsByParamName("CB_RSTK_GL_" + valueCompTag);
                    if (wrapper.IsSuccess == false)
                    {
                        scope.Dispose();
                        return wrapper;
                    }

                    GLSettingsModel glSetting = wrapper.ResultSet[0] as GLSettingsModel;
                    if (!string.IsNullOrEmpty(glSetting.ParamValChar))
                    {
                        Setting_CB_RSTK_GL = glSetting.ParamValChar;
                    }
                    else
                    {
                        scope.Dispose();
                        wrapper.ResultSet.Clear();
                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add($"SaveRMReturn: gl parameter CB_RSTK_GL_ {valueCompTag} not in settings.");
                        return wrapper;
                    }

                    //--RM stock Adjustmet account 
                    wrapper = _glAccountService.GetJobSettingsByParamName("CB_STAJ_GL_" + valueCompTag);
                    if (wrapper.IsSuccess == false)
                    {
                        scope.Dispose();
                        return wrapper;
                    }

                    glSetting = wrapper.ResultSet[0] as GLSettingsModel;
                    if (!string.IsNullOrEmpty(glSetting.ParamValChar))
                    {
                        Setting_CB_STAJ_GL = glSetting.ParamValChar;
                    }
                    else
                    {
                        scope.Dispose();
                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add($"SaveRMReturn: gl parameter CB_STAJ_GL_ {valueCompTag} not in settings.");
                        return wrapper;
                    }

                    //--work in progress
                    wrapper = _glAccountService.GetJobSettingsByParamName("CB_WIP_GL_" + valueCompTag);
                    if (wrapper.IsSuccess == false)
                    {
                        scope.Dispose();
                        return wrapper;
                    }

                    glSetting = wrapper.ResultSet[0] as GLSettingsModel;
                    if (!string.IsNullOrEmpty(glSetting.ParamValChar))
                    {
                        Setting_CB_WIP_GL = glSetting.ParamValChar;
                    }
                    else
                    {
                        scope.Dispose();
                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add($"SaveRMReturn: gl parameter CB_WIP_GL_ {valueCompTag} not in settings.");
                        return wrapper;
                    }

                    //--RM usage variance
                    wrapper = _glAccountService.GetJobSettingsByParamName("CB_RMV_GL_" + valueCompTag);
                    if (wrapper.IsSuccess == false)
                    {
                        scope.Dispose();
                        return wrapper;
                    }

                    glSetting = wrapper.ResultSet[0] as GLSettingsModel;
                    if (!string.IsNullOrEmpty(glSetting.ParamValChar))
                    {
                        Setting_CB_RMV_GL = glSetting.ParamValChar;
                    }
                    else
                    {
                        scope.Dispose();
                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add($"SaveRMReturn: gl parameter CB_RMV_GL_ {valueCompTag} not in settings.");
                        return wrapper;
                    }

                    ////--Cost period by Date
                    //wrapper = _jobService.GetCostPeriodByDate(DateTime.Now);
                    //if (wrapper.IsSuccess == false)
                    //{
                    //    scope.Dispose();
                    //    return wrapper;
                    //}

                    //JobCostPeriodModel jCostPeriod = wrapper.ResultSet[0] as JobCostPeriodModel;
                    //if (jCostPeriod.CostPeriod == 0)
                    //{
                    //    scope.Dispose();
                    //    wrapper.IsSuccess = false;
                    //    wrapper.Messages.Add($"SaveRMReturn: Cost period and Cost year is not defined for " + DateTime.Now.ToShortDateString());
                    //    return wrapper;
                    //}

                    FinancialYear financialObj = new FinancialYear();
                    JobCostPeriodModel jCostPeriod = financialObj.GetFinancialYear(DateTime.Now);

                    /* rm details */
                    double RateTonne = _rawMaterialsService.GetRateTonneByCatalogCode(retunrModel.CatalogCode);
                    if (RateTonne < 0)
                    {
                        scope.Dispose();
                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add("GetRateTonneByCatalogCode : Error");
                        return wrapper;
                    }

                    /* IF we know what the tag is THEN we can find out the job details */
                    if (retunrModel.TagId == "")
                    {
                    }
                    else if ((retunrModel.JobNo == 0 || retunrModel.CatalogCode == "" || retunrModel.CostItemNo == 0) && retunrModel.TagId != "")
                    {
                        wrapper = _jobService.GetJobDetailByTagId(retunrModel.TagId);
                        if (wrapper.IsSuccess == false)
                        {
                            scope.Dispose();
                            return wrapper;
                        }

                        JobDetailsRMModel detail = wrapper.ResultSet[0] as JobDetailsRMModel;
                    }

                    /* If we know what job details that it went out from */
                    if (retunrModel.JobNo != 0 && retunrModel.CatalogCode != "" && retunrModel.CostItemNo != 0)
                    {
                        //reduce the qty in the job 
                        //UPDATE the usage 
                        wrapper = _jobService.AddUsedQty(retunrModel.JobNo,
                            retunrModel.CatalogCode,
                            retunrModel.CostItemNo,
                            -1 * retunrModel.RetQty);
                        if (wrapper.IsSuccess == false)
                        {
                            scope.Dispose();
                            return wrapper;
                        }

                        //--INSERT record INTO actions
                        JobOrderLogModel jobLog = new JobOrderLogModel();
                        jobLog.JobNo = retunrModel.JobNo;
                        jobLog.Action = "RETURN " + retunrModel.RetQty.ToString();
                        jobLog.ActionDate = DateTime.Now;
                        jobLog.CatalogCode = retunrModel.CatalogCode;
                        jobLog.CostItemNo = retunrModel.CostItemNo;
                        jobLog.Originator = retunrModel.Originator;
                        jobLog.ItemType = "";
                        jobLog.BatchNo = 0;

                        wrapper = _jobService.SaveJobOrderLog(jobLog);
                        if (wrapper.IsSuccess == false)
                        {
                            scope.Dispose();
                            return wrapper;
                        }
                    }

                    /* Insert into Pallet header and detail */
                    string preWh = "";
                    if (retunrModel.WarehouseId == "MR")
                    {
                        preWh = "M1.";
                    }
                    else
                    {
                        preWh = "E1.";
                    }

                    //--Create Pallet Header
                    PalletHeader palletHeader = new PalletHeader();
                    palletHeader.PalletNumber = 0;
                    palletHeader.PlanNumber = -3;
                    palletHeader.TransferStatus = "T";
                    palletHeader.WarehouseId = retunrModel.WarehouseId;
                    palletHeader.Status = "W";
                    palletHeader.Quality = "G";
                    palletHeader.PrintedAt = "HH-" + retunrModel.Originator;
                    palletHeader.PrintDate = DateTime.Now;
                    palletHeader.BinLocation = preWh + retunrModel.WarehouseId + ".BULK";
                    palletHeader.PickingLabel = retunrModel.TagId;

                    List<PalletDetail> palletDetails = new List<PalletDetail>();
                    PalletDetail palletDetail = new PalletDetail();
                    palletDetail.PalletNumber = 0;
                    palletDetail.CatalogCode = retunrModel.CatalogCode.Trim();
                    palletDetail.OldPalletNumber = 0;
                    palletDetail.OriginalPalletUnits = Convert.ToInt32(retunrModel.RetQty);
                    palletDetail.PalletUnits = Convert.ToInt32(retunrModel.RetQty);
                    palletDetail.StockQty = retunrModel.RetQty;
                    palletDetail.PalletQuantity = retunrModel.RetQty;
                    palletDetail.BatchNumber = -3;
                    palletDetail.BestBefore = DateTime.MaxValue;
                    palletDetail.WarehouseId = retunrModel.WarehouseId;
                    palletDetails.Add(palletDetail);

                    palletHeader.PalletDetails = palletDetails;

                    int newPalletNumber = 0;

                    wrapper = _palletService.CreatePalletHeader(palletHeader, ref newPalletNumber);
                    if (wrapper.IsSuccess == false)
                    {
                        scope.Dispose();
                        return wrapper;
                    }

                    //--Stock Movements 
                    StockDocketModel docketModel = new StockDocketModel();
                    docketModel.CatalogCode = retunrModel.CatalogCode;
                    docketModel.WarehouseId = retunrModel.WarehouseId;
                    docketModel.MoveType = "BATC";
                    docketModel.MoveDate = DateTime.Now;
                    docketModel.MoveQty = -1 * retunrModel.RetQty;
                    docketModel.RateTonne = RateTonne;
                    docketModel.Narration = "Job No " + retunrModel.JobNo.ToString() + ' ' + palletHeader.BinLocation;
                    docketModel.JobNo = retunrModel.JobNo;
                    docketModel.Originator = retunrModel.Originator;

                    wrapper = _stockService.SaveStockDocket(docketModel);
                    if (wrapper.IsSuccess == false)
                    {
                        scope.Dispose();
                        return wrapper;
                    }

                    errorWrapper.Messages.Add("Creating GL Models");
                    /* THE GL Transactions */
                    GLBatchHeaderModel glBatch = new GLBatchHeaderModel();
                    glBatch.Description = "RM Prod stk Return for " + retunrModel.JobNo.ToString();
                    glBatch.Source = "AR";

                    GLJnlDetailModel glJnlDetail = new GLJnlDetailModel();
                    glJnlDetail.FinYear = jCostPeriod.CostYear;
                    glJnlDetail.FinPeriod = jCostPeriod.CostPeriod;
                    glJnlDetail.JnlDate = DateTime.Now;
                    glJnlDetail.EntryNo = 1;

                    glBatch.JnlDetails = new List<GLJnlDetailModel>
                    {
                        glJnlDetail
                    };
                    //--GL details
                    //--get the gl account FOR the rm code 
                    string RmGLAccount = "";
                    wrapper = _catalogService.GetCatalogByCatalogCode(retunrModel.CatalogCode);
                    if (wrapper.IsSuccess == false)
                    {
                        scope.Dispose();
                        return wrapper;
                    }
                    errorWrapper.Messages.Add("Getting Catalog details");
                    Catalog catalogObj = wrapper.ResultSet[0] as Catalog;
                    if (String.IsNullOrEmpty(catalogObj.GLAccount))
                    {
                        RmGLAccount = Setting_CB_RSTK_GL;
                    }
                    else
                    {
                        RmGLAccount = catalogObj.GLAccount;
                    }

                    errorWrapper.Messages.Add("Debit Stock");
                    //debit stock 
                    GLJnlGLDetailModel glJnlGLDetail = new GLJnlGLDetailModel();
                    glJnlGLDetail.SourceType = "PRO";
                    glJnlGLDetail.SourceDoc = retunrModel.JobNo.ToString();
                    glJnlGLDetail.SourceCurrency = Currency;
                    glJnlGLDetail.JnlDB = retunrModel.RetQty * RateTonne;
                    glJnlGLDetail.ExchRate = 1;
                    glJnlGLDetail.GLAccount = RmGLAccount;
                    glJnlGLDetail.SourceTransDate = DateTime.Now;
                    glJnlGLDetail.Quantity = retunrModel.RetQty;
                    glJnlGLDetail.Reference = "Rm Usage Job: " + retunrModel.JobNo.ToString() + " " + retunrModel.CatalogCode;
                    glJnlGLDetail.Comment = "Rm Usage Job: " + retunrModel.JobNo.ToString() + ' ' + retunrModel.CatalogCode;

                    glJnlDetail.JnlGLDetails = new List<GLJnlGLDetailModel>
                    {
                        glJnlGLDetail
                    };

                    double Lf_tot = retunrModel.RetQty * RateTonne;
                    errorWrapper.Messages.Add("GLDetail If Statement");
                    if (retunrModel.JobNo > 0)
                    {
                        // credit WIP 
                        glJnlGLDetail.SourceType = "PRO";
                        glJnlGLDetail.SourceDoc = retunrModel.JobNo.ToString();
                        glJnlGLDetail.SourceCurrency = Currency;
                        glJnlGLDetail.ExchRate = 1;
                        glJnlGLDetail.GLAccount = Setting_CB_WIP_GL;
                        glJnlGLDetail.JnlCR = Lf_tot;
                        glJnlGLDetail.SourceTransDate = DateTime.Now;
                        glJnlGLDetail.Quantity = retunrModel.RetQty;
                        glJnlGLDetail.Reference = "Rm return: " + retunrModel.JobNo.ToString() + " " + retunrModel.CatalogCode;
                        glJnlGLDetail.Comment = "Rm return: " + retunrModel.JobNo.ToString() + " " + retunrModel.CatalogCode;
                    }
                    else
                    {
                        // credit stock variance 
                        glJnlGLDetail.SourceType = "PRO";
                        glJnlGLDetail.SourceDoc = retunrModel.JobNo.ToString();
                        glJnlGLDetail.SourceCurrency = Currency;
                        glJnlGLDetail.ExchRate = 1;
                        glJnlGLDetail.GLAccount = Setting_CB_STAJ_GL;
                        glJnlGLDetail.JnlCR = Lf_tot;
                        glJnlGLDetail.SourceTransDate = DateTime.Now;
                        glJnlGLDetail.Quantity = retunrModel.RetQty;
                        glJnlGLDetail.Reference = "Rm return: " + retunrModel.JobNo.ToString() + " " + retunrModel.CatalogCode;
                        glJnlGLDetail.Comment = "Rm return: " + retunrModel.JobNo.ToString() + " " + retunrModel.CatalogCode;
                    }

                    errorWrapper.Messages.Add("Saving GL Transaction");
                    glJnlDetail.JnlGLDetails.Add(glJnlGLDetail);

                    wrapper = _glAccountService.SaveJnlGLTransactions(glBatch);
                    if (wrapper.IsSuccess == false)
                    {
                        scope.Dispose();
                        return wrapper;
                    }

                    scope.Complete();
                }
                catch (Exception e)
                {
                    scope.Dispose();
                    errorWrapper.IsSuccess = false;
                    errorWrapper.Messages.Add(e.Message);
                    return errorWrapper;
                }
            }

            return wrapper;
        }

        public TransactionWrapper CheckPalletLabelExist(string rmCatalogCode, int jobNo, string tagId)
        {
            int rowCount = 0;
            int tagExist = 0;

            TransactionWrapper wrapper = _palletService.CheckPalletLabelExist(rmCatalogCode, jobNo, tagId, ref rowCount, ref tagExist);
            if (wrapper.IsSuccess == false)
            {
                return wrapper;
            }

            if (tagExist == 1)
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("TagId not valid or this item not issued to this job");
                return wrapper;
            }

            return wrapper;

        }
    }
}
