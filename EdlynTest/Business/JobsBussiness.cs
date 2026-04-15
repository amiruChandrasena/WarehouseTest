using Abstractions.ServiceInterfaces;
using Common;
using Models;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics;
using System.Text;
using System.Transactions;

namespace Business
{
    public class JobsBussiness
    {
        private readonly IJobService _jobService;
        private readonly IRawMaterialService _rawMaterialsService;
        private readonly IPalletService _palletService;
        private readonly ICatalogService _catalogService;
        private readonly IGLAccountService _glAccountService;
        private readonly IStockService _stockService;

        public JobsBussiness(IJobService jobService, IRawMaterialService rawMaterialsService,
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

        public TransactionWrapper GetJobByJobNo(int jobNo, bool isReturn)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            wrapper = _jobService.GetRMJobDetailsByJobNo(jobNo);
            if (wrapper.IsSuccess == false)
            {
                return wrapper;
            }

            JobHeaderRMModel header = wrapper.ResultSet[0] as JobHeaderRMModel;

            header.ByProductUom = _rawMaterialsService.GetUOMByProductCode(header.ByProductCode);

            if (header.PackTime == null)
            {
                header.PackTime = header.PackDate;
            }

            if (header.EndTime == null)
            {
                header.EndTime = header.EndDate;
            }

            //Add this foreeach By Irosh 2021/09/07 - Email - "Need to Fix issue in Raw Material new gun"
            //Canceled and closed job will not come to GetJobByJobNo
            //X - Cancelled
            //C - Closed
            List<string> list = new List<string>();
            if (!isReturn)
            {
                list.Add("C");
                list.Add("X");
            }
            else
            {
                list.Add("C");
            }

            string[] IgnoreStatus = list.ToArray();

            foreach (string x in IgnoreStatus)
            {
                if (header.Status.Contains(x))
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetJobByJobNo(int): This job no " + jobNo.ToString() + " already cancelled / closed. ");
                    return wrapper;
                }
            }

            //IF job type is rework job then get the rework instructions 
            if (header.JobType == "R")
            {
                header.ReworkInstructions = _jobService.GetRMReworkInstructionByJobNo(jobNo);
            }
            else
            {
                header.ReworkInstructions = "";
            }

            //Not intermediate (FG) 
            if (header.JobType != "I")
            {
                //--not closed and a real job
                if ((header.Status != "C") && (header.JobNo != 0))
                {
                    var ActualUnits = _palletService.GetActualPalletUnitsByJobNo(jobNo);
                    if (ActualUnits != 0)
                    {
                        header.ActualQtyUnit = ActualUnits;
                    }
                }

                if ((header.ActualQtyUnit != 0) && (header.ActualQty == 0))
                {
                    header.Conversion = 0;

                    header.Conversion = _rawMaterialsService.GetUOMConvertionByCatalogCode(header.CatalogCode);

                    header.ActualQty = header.ActualQtyUnit * header.Conversion * 1000;
                }

                if (header.PlanQty > 0)
                {
                    header.Yeild = (header.ActualQty / header.PlanQty) * 100;
                }
            }
            else
            {
                /*Coversions, FG Conversions (in the FG uom TABLE use a base unit of Tonne) 
                RM uses a relative methedlology, 
                IF the job order is a finished goods Job order THEN uses the FG UOM tabel 
                otherwise use the RM uom*/

                header.Conversion = 1;
                header.ActualQty = header.ActualQtyUnit;
            }

            wrapper = LoadDetailsforJobByJobNo(jobNo, "");
            if (wrapper.IsSuccess == false)
            {
                return wrapper;
            }

            List<JobDetailsRMModel> details = wrapper.ResultSet[0] as List<JobDetailsRMModel>;
            wrapper.ResultSet.Clear();


            if (details.Count > 0)
                header.JobDetails = details;

            wrapper.IsSuccess = true;
            wrapper.ResultSet.Add(header);

            return wrapper;
        }

        //Have to Check
        public TransactionWrapper LoadDetailsforJobByJobNo(int jobNo, string CatalogCode)
        {
            TransactionWrapper wrapper = _jobService.GetAllJobDetailsByJobNo(jobNo, CatalogCode);

            return wrapper;
        }

        public TransactionWrapper GetAllJobsSearchList(int jobNo, string lineNo, string catalogCode, DateTime fromDate, DateTime toDate, bool isReturn)
        {
            string strStatus = "'N','A','R','I'";

            TransactionWrapper wrapper = new TransactionWrapper();

            if (isReturn)
            {
                strStatus = "'N','A','R','I','C'";
            }

            wrapper = _jobService.GetAllJobsSearchList(jobNo: jobNo,
                lineNo: lineNo,
                catalogCode: catalogCode,
                fromDate: fromDate,
                toDate: toDate,
                status: strStatus,
                glUpdate: 0);

            return wrapper;
        }

        public TransactionWrapper JobIssueSave(JobIssueModel issueTrans)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            using (TransactionScope scope = new TransactionScope())
            {
                wrapper = IssueToJob(issueTrans.Originator,
                    issueTrans.JobNo,
                    issueTrans.CatalogCode,
                    issueTrans.CostItemNo,
                    issueTrans.PalletNo,
                    issueTrans.AvailableQty,
                    issueTrans.IssueQty,
                    issueTrans.Currency);

                if (wrapper.IsSuccess)
                    scope.Complete();
                else
                    scope.Dispose();
            }

            return wrapper;
        }

        private TransactionWrapper IssueToJob(string originator, int jobNo, string catalogCode, int costItemNo, int palletNo, double availableQty, double issueQty, string currency)
        {
            string strMethodName = string.Empty;
            string CompanyTag = "1";
            string Setting_CB_RSTK_GL = "";
            string Setting_CB_WIP_GL = "";
            string Pallet_Lv_Whouse = "";

            TransactionWrapper wrapper = new TransactionWrapper();

            string uniqNo = DateTime.Now.ToString("yyMMddhhmmssms");
            string logFileName = String.Format("JobIssueSave_Test_{0}_{1}.txt", DateTime.Now.ToString("yyyyMMdd"), jobNo.ToString());

            try
            {
                WriteLogFile.WriteLog(logFileName, String.Format("{0} {1} {2}", DateTime.Now, uniqNo, "Start Job Issue Save"));

                //--get the job detail 
                wrapper = _jobService.GetRMJobDetailsByJobNo(jobNo);
                if (wrapper.IsSuccess == false)
                {
                    return wrapper;
                }

                WriteLogFile.WriteLog(logFileName, String.Format("{0} {1} {2}", DateTime.Now, uniqNo, "Complete GetRMJobDetailsByJobNo"));

                JobHeaderRMModel jHeader = wrapper.ResultSet[0] as JobHeaderRMModel;

                #region "Get PU Catalog Account Settings"

                //--get company code by catalog code
                wrapper = _catalogService.GetCatalogByCatalogCode(catalogCode);
                if (wrapper.IsSuccess == false)
                {
                    return wrapper;
                }

                try
                {
                    Catalog catalog = wrapper.ResultSet[0] as Catalog;
                    CompanyTag = catalog.GLAccount.Split('.')[0];
                }
                catch { CompanyTag = "1"; }

                //--get the GL settings
                //--RM stock Account 
                wrapper = _glAccountService.GetJobSettingsByParamName("CB_RSTK_GL_" + CompanyTag);
                if (wrapper.IsSuccess == false)
                {
                    return wrapper;
                }

                WriteLogFile.WriteLog(logFileName, String.Format("{0} {1} {2}", DateTime.Now, uniqNo, "Complete GetJobSettingsByParamName CB_RSTK_GL_" + CompanyTag));

                GLSettingsModel jSetting = wrapper.ResultSet[0] as GLSettingsModel;
                if (!string.IsNullOrEmpty(jSetting.ParamValChar))
                {
                    Setting_CB_RSTK_GL = jSetting.ParamValChar;
                }
                else
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("CB_RSTK_GL_" + CompanyTag + " Setting is empty");
                    return wrapper;
                }

                #endregion

                #region "Get Catalog Account Settings"

                //--get company code by catalog code
                wrapper = _catalogService.GetCatalogByCatalogCodeForGLAccount(jobNo);
                if (wrapper.IsSuccess == false)
                {
                    return wrapper;
                }

                try
                {
                    Catalog catalog = wrapper.ResultSet[0] as Catalog;
                    CompanyTag = catalog.GLAccount.Split('.')[0];
                }
                catch {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("CB_WIP_GL_" + CompanyTag + " Setting is empty");
                    return wrapper;
                }

                //--work in progress
                wrapper = _glAccountService.GetJobSettingsByParamName("CB_WIP_GL_" + CompanyTag);
                if (wrapper.IsSuccess == false)
                {
                    return wrapper;
                }

                WriteLogFile.WriteLog(logFileName, String.Format("{0} {1} {2}", DateTime.Now, uniqNo, "Complete GetJobSettingsByParamName CB_WIP_GL_" + CompanyTag));

                jSetting = wrapper.ResultSet[0] as GLSettingsModel;
                if (!string.IsNullOrEmpty(jSetting.ParamValChar))
                {
                    Setting_CB_WIP_GL = jSetting.ParamValChar;
                }
                else
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("CB_WIP_GL_" + CompanyTag + " Setting is empty");
                    return wrapper;
                }

                #endregion

                ////--Cost period by Date
                //wrapper = _jobService.GetCostPeriodByDate(DateTime.Now);
                //if (wrapper.IsSuccess == false)
                //{
                //    scope.Dispose();
                //    return wrapper;
                //}

                //JobCostPeriodModel jCostPeriod = wrapper.ResultSet[0] as JobCostPeriodModel;

                FinancialYear financialObj = new FinancialYear();
                JobCostPeriodModel jCostPeriod = financialObj.GetFinancialYear(DateTime.Now);

                WriteLogFile.WriteLog(logFileName, String.Format("{0} {1} {2}", DateTime.Now, uniqNo, "Complete GetCostPeriodByDate"));

                //--which warehouse is the pallet in ? 

                wrapper = _palletService.GetPalletDetailForJobIssueSave(palletNo);
                if (wrapper.IsSuccess == false)
                {
                    return wrapper;
                }

                WriteLogFile.WriteLog(logFileName, String.Format("{0} {1} {2}", DateTime.Now, uniqNo, "Complete GetPalletDetail 1"));

                PalletHeader palletHeader = wrapper.ResultSet[0] as PalletHeader;
                if (wrapper.ResultSet.Count == 0)
                {
                    wrapper = _palletService.GetPalletNoRMByPickingLabel(palletNo);
                    if (wrapper.IsSuccess == false)
                    {
                        return wrapper;
                    }

                    PalletHeader palletNumber = wrapper.ResultSet[0] as PalletHeader;
                    WriteLogFile.WriteLog(logFileName, String.Format("{0} {1} {2}", DateTime.Now, uniqNo, "Complete GetPalletNoRMByPalletNoOrPalletLabel"));

                    wrapper = _palletService.GetPalletDetail(palletNumber.PalletNumber);
                    if (wrapper.IsSuccess == false)
                    {
                        return wrapper;
                    }

                    WriteLogFile.WriteLog(logFileName, String.Format("{0} {1} {2}", DateTime.Now, uniqNo, "Complete GetPalletDetail 2"));

                    palletHeader = wrapper.ResultSet[0] as PalletHeader;
                }

                Pallet_Lv_Whouse = palletHeader.WarehouseId;

                //--the required quantity
                List<JobDetailsRMModel> jobDetailsReqQty = new List<JobDetailsRMModel>();
                if (costItemNo > 0)
                {
                    wrapper = _jobService.GetAllJobDetailsByJobNo(jobNo, costItemNo, catalogCode);
                    strMethodName = "GetAllJobDetailsByJobNo-1";
                }
                else
                {
                    wrapper = _jobService.GetAllJobDetailsByJobNo(jobNo, 0, catalogCode);
                    strMethodName = "GetAllJobDetailsByJobNo-2";
                }

                if (wrapper.IsSuccess == false)
                {
                    return wrapper;
                }

                WriteLogFile.WriteLog(logFileName, String.Format("{0} {1} {2}", DateTime.Now, uniqNo, "Complete " + strMethodName));

                jobDetailsReqQty = wrapper.ResultSet[0] as List<JobDetailsRMModel>;

                //--rm details
                double RateTonne = _rawMaterialsService.GetRateTonneByCatalogCode(catalogCode);

                WriteLogFile.WriteLog(logFileName, String.Format("{0} {1} {2}", DateTime.Now, uniqNo, "Complete GetRateTonneByCatalogCode"));

                JobMixHeader mixHeader = new JobMixHeader();
                mixHeader.CatalogCode = jHeader.CatalogCode;
                mixHeader.JobNo = jHeader.JobNo;
                mixHeader.MixDate = DateTime.Now;
                mixHeader.MixNo = 0;
                mixHeader.Originator = originator;
                mixHeader.Type = "B";

                List<JobMixDetail> mixDetailList = new List<JobMixDetail>();
                JobMixDetail mixDetail = new JobMixDetail();
                mixDetail.CatalogCode = catalogCode;
                mixDetail.JobNo = jobNo;
                mixDetail.SplitNo = 1;
                mixDetail.LotId = palletNo;
                mixDetail.ReqQty = jobDetailsReqQty[0].ReqQty;
                mixDetail.IssueQty = issueQty;
                mixDetailList.Add(mixDetail);

                mixHeader.JobMixDetailList = mixDetailList;

                TransactionWrapper twMixSaveStatus = _jobService.SaveJobMix(mixHeader);
                if (twMixSaveStatus.IsSuccess == false)
                {
                    return twMixSaveStatus;
                }

                WriteLogFile.WriteLog(logFileName, String.Format("{0} {1} {2}", DateTime.Now, uniqNo, "Complete SaveJobMix"));

                //--this is a lazy way to do it I know - fix later 
                TransactionWrapper twUpdatePFJobMixDetailStatus = _jobService.UpdatePFJobMixDetail(mixHeader);
                if (twUpdatePFJobMixDetailStatus.IsSuccess == false)
                {
                    return twUpdatePFJobMixDetailStatus;
                }

                WriteLogFile.WriteLog(logFileName, String.Format("{0} {1} {2}", DateTime.Now, uniqNo, "Complete UpdatePFJobMixDetail"));

                //--Stock Movements 
                StockDocketModel docketModel = new StockDocketModel
                {
                    CatalogCode = catalogCode,
                    WarehouseId = palletHeader.WarehouseId,
                    MoveType = "BATC",
                    MoveDate = DateTime.Now,
                    MoveQty = issueQty,
                    RateTonne = float.Parse(RateTonne.ToString()),
                    Narration = "Job No " + jobNo.ToString() + ' ' + palletHeader.BinLocation,
                    JobNo = jobNo,
                    Originator = originator
                };

                TransactionWrapper twSaveStockDocketStatus = _stockService.SaveStockDocket(docketModel);
                if (twSaveStockDocketStatus.IsSuccess == false)
                {
                    return twSaveStockDocketStatus;
                }

                WriteLogFile.WriteLog(logFileName, String.Format("{0} {1} {2}", DateTime.Now, uniqNo, "Complete SaveStockDocket"));

                //--UPDATE the pallet 
                PalletDetail palletDetail = palletHeader.PalletDetails[0];

                TransactionWrapper twUpdatePalletStatus = _palletService.UpdatePalletQuantity(palletNo, palletDetail.StockQty, issueQty, catalogCode);
                if (twUpdatePalletStatus.IsSuccess == false)
                {
                    return twUpdatePalletStatus;
                }

                WriteLogFile.WriteLog(logFileName, String.Format("{0} {1} {2}", DateTime.Now, uniqNo, "Complete UpdatePalletQuantity"));

                //Check pallet is fully issued
                if ((availableQty - issueQty) < 0.01)
                {
                    wrapper = _palletService.UpdatePalletHeaderStatusByPalletNo(palletNo, "D");
                    if (wrapper.IsSuccess == false)
                    {
                        return wrapper;
                    }

                    WriteLogFile.WriteLog(logFileName, String.Format("{0} {1} {2}", DateTime.Now, uniqNo, "Complete UpdatePalletHeaderStatusByPalletNo"));
                }

                //--UPDATE The job order , UPDATE the usage 
                TransactionWrapper twAddUsedQtyStatus = _jobService.AddUsedQty(jobNo, catalogCode, costItemNo, issueQty);
                if (twAddUsedQtyStatus.IsSuccess == false)
                {
                    return twAddUsedQtyStatus;
                }

                WriteLogFile.WriteLog(logFileName, String.Format("{0} {1} {2}", DateTime.Now, uniqNo, "Complete AddUsedQty"));

                //--change the status 
                TransactionWrapper twUpdateJobHeaderStatus = _jobService.UpdateJobHeaderStatus(jobNo, "I");
                if (twUpdateJobHeaderStatus.IsSuccess == false)
                {
                    return twUpdateJobHeaderStatus;
                }

                WriteLogFile.WriteLog(logFileName, String.Format("{0} {1} {2}", DateTime.Now, uniqNo, "Complete UpdateJobHeaderStatus"));

                //--INSERT record INTO actions
                JobOrderLogModel jobLog = new JobOrderLogModel();
                jobLog.JobNo = jobNo;
                jobLog.Action = "ISSUE " + issueQty.ToString();
                jobLog.ActionDate = DateTime.Now;
                jobLog.CatalogCode = catalogCode;
                jobLog.CostItemNo = costItemNo;
                jobLog.Originator = originator;
                jobLog.ItemType = "";
                jobLog.BatchNo = 0;

                TransactionWrapper twSaveJobOrderLogStatus = _jobService.SaveJobOrderLog(jobLog);
                if (twSaveJobOrderLogStatus.IsSuccess == false)
                {
                    return twSaveJobOrderLogStatus;
                }

                WriteLogFile.WriteLog(logFileName, String.Format("{0} {1} {2}", DateTime.Now, uniqNo, "Complete SaveJobOrderLog"));

                /* THE GL Transactions */
                GLBatchHeaderModel glBatch = new GLBatchHeaderModel();
                glBatch.JnlDetails = new List<GLJnlDetailModel>();
                glBatch.Description = "RM Usage for " + jobNo.ToString();
                glBatch.Source = "AR";
                glBatch.CompanyCode = CompanyTag;
                glBatch.Originator = originator;

                GLJnlDetailModel glJnlDetail = new GLJnlDetailModel();
                glJnlDetail.JnlGLDetails = new List<GLJnlGLDetailModel>();
                glJnlDetail.FinYear = jCostPeriod.CostYear;
                glJnlDetail.FinPeriod = jCostPeriod.CostPeriod;
                glJnlDetail.JnlDate = DateTime.Now;
                glJnlDetail.EntryNo = 1;

                //--GL details
                //--get the gl account FOR the rm code 
                string RmGLAccount = "";
                wrapper = _catalogService.GetCatalogByCatalogCode(catalogCode);
                if (wrapper.IsSuccess == false)
                {
                    return wrapper;
                }

                WriteLogFile.WriteLog(logFileName, String.Format("{0} {1} {2}", DateTime.Now, uniqNo, "Complete GetCatalogByCatalogCode"));

                Catalog catalogObj = wrapper.ResultSet[0] as Catalog;
                if (catalogObj.GLAccount == "")
                {
                    RmGLAccount = Setting_CB_RSTK_GL;
                }
                else
                {
                    RmGLAccount = catalogObj.GLAccount;
                }

                GLJnlGLDetailModel glJnlGLDetail = new GLJnlGLDetailModel();
                glJnlGLDetail.SourceType = "PRO";
                glJnlGLDetail.SourceDoc = jobNo.ToString();
                glJnlGLDetail.SourceCurrency = currency;
                glJnlGLDetail.JnlCR = double.Parse(issueQty.ToString()) * RateTonne;
                glJnlGLDetail.ExchRate = 1;
                glJnlGLDetail.GLAccount = RmGLAccount;
                glJnlGLDetail.SourceTransDate = DateTime.Now;
                glJnlGLDetail.Quantity = issueQty;
                glJnlGLDetail.Reference = "Rm Usage Job: " + jobNo.ToString() + " " + catalogCode;
                glJnlGLDetail.Comment = "Rm Usage Job: " + jobNo.ToString() + ' ' + catalogCode;

                glJnlDetail.JnlGLDetails.Add(glJnlGLDetail);
                string Setting_CB_RMV_GL = "";
                double Lf_VarQty = 0;
                double Lf_tot = issueQty * RateTonne;
                double Lf_var = Lf_VarQty * RateTonne;

                glJnlGLDetail = new GLJnlGLDetailModel();
                glJnlGLDetail.SourceType = "PRO";
                glJnlGLDetail.SourceDoc = jobNo.ToString();
                glJnlGLDetail.SourceCurrency = currency;
                glJnlGLDetail.ExchRate = 1;
                glJnlGLDetail.GLAccount = Setting_CB_WIP_GL;
                glJnlGLDetail.JnlDB = Lf_tot;
                glJnlGLDetail.SourceTransDate = DateTime.Now;
                glJnlGLDetail.Quantity = issueQty;
                glJnlGLDetail.Reference = "Rm Usage Job: " + jobNo.ToString() + " " + catalogCode;
                glJnlGLDetail.Comment = "Rm Usage Job: " + jobNo.ToString() + " " + catalogCode;

                glJnlDetail.JnlGLDetails.Add(glJnlGLDetail);

                /*
                if (Lf_var > 0)
                {
                    glJnlGLDetail.SourceType = "PRO";
                    glJnlGLDetail.SourceDoc = jobNo.ToString();
                    glJnlGLDetail.SourceCurrency = currency;
                    glJnlGLDetail.ExchRate = 1;
                    glJnlGLDetail.GLAccount = Setting_CB_WIP_GL;
                    glJnlGLDetail.JnlCR = Lf_var;
                    glJnlGLDetail.SourceTransDate = DateTime.Now;
                    glJnlGLDetail.Reference = "Rm Usage Var Job: " + jobNo.ToString() + " " + catalogCode;

                    glJnlDetail.JnlGLDetails.Add(glJnlGLDetail);

                    glJnlGLDetail.SourceType = "PRO";
                    glJnlGLDetail.SourceDoc = jobNo.ToString();
                    glJnlGLDetail.SourceCurrency = currency;
                    glJnlGLDetail.ExchRate = 1;
                    glJnlGLDetail.GLAccount = Setting_CB_RMV_GL;
                    glJnlGLDetail.JnlCR = Lf_var;
                    glJnlGLDetail.SourceTransDate = DateTime.Now;
                    glJnlGLDetail.Reference = "Rm Usage Var Job: " + jobNo.ToString() + " " + catalogCode;

                    glJnlDetail.JnlGLDetails.Add(glJnlGLDetail);
                } */
                glBatch.JnlDetails.Add(glJnlDetail);
                TransactionWrapper twSaveJnlGLTransactionsStatus = _glAccountService.SaveJnlGLTransactions(glBatch);
                if (twSaveJnlGLTransactionsStatus.IsSuccess == false)
                {
                    return twSaveJnlGLTransactionsStatus;
                }

                WriteLogFile.WriteLog(logFileName, String.Format("{0} {1} {2}", DateTime.Now, uniqNo, "Complete SaveJnlGLTransactions"));

                //glBatch = twSaveJnlGLTransactionsStatus.ResultSet[0] as GLBatchHeaderModel;

                wrapper.ResultSet.Clear();
                wrapper.ResultSet.Add(glBatch);
                wrapper.IsSuccess = true;

                WriteLogFile.WriteLog(logFileName, String.Format("{0} {1} {2}", DateTime.Now, uniqNo, "Stop Job Issue Save"));

                return wrapper;
            }
            catch (Exception e)
            {
                WriteLogFile.WriteLog(logFileName, String.Format("{0} {1} {2}", DateTime.Now, uniqNo, "Error Job Issue Save"));

                wrapper.IsSuccess = false;
                wrapper.Messages.Add(e.Message);
                return wrapper;
            }
        }
    }
}

