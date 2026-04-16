using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Abstractions.ServiceInterfaces;
using Business;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Models;

namespace WarehouseScannerApi.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class JobsController : ControllerBase
    {
        private readonly IRawMaterialService _rawMaterialService;
        private readonly IJobService _jobService;
        private readonly IPalletService _palletService;
        private readonly ICatalogService _catalogService;
        private readonly IGLAccountService _glAccountService;
        private readonly IStockService _stockService;
        private readonly JobsBussiness _jobBussiness;

        public JobsController(IJobService jobService, IRawMaterialService rawMaterialService, 
            IPalletService palletService, ICatalogService catalogService, 
            IGLAccountService glAccountService, IStockService stockService)
        {
            _jobService = jobService;
            _rawMaterialService = rawMaterialService;
            _palletService = palletService;
            _catalogService = catalogService;
            _glAccountService = glAccountService;
            _stockService = stockService;
            _jobBussiness = new JobsBussiness(_jobService, _rawMaterialService, _palletService, _catalogService, _glAccountService, _stockService);
        }

        [Route("getrmjobdetails/{jobNo}/{isReturn?}")]
        [HttpGet]
        public ActionResult<TransactionWrapper> GetRMJobDetailsByJobNo(int jobNo, int? isReturn = 0)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            if (jobNo == 0)
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("GetRMJobDetailsByJobNo: Fail due to missing jobNo");
                return wrapper;
            }
            else
            {
                wrapper = _jobBussiness.GetJobByJobNo(jobNo, Convert.ToBoolean(isReturn));
                return wrapper;
            }
        }

        [Route("getrmjobsearchresults/{isReturn?}")]
        [HttpGet]
        public ActionResult<TransactionWrapper> GetAllJobsSearchList(int? isReturn = 0)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            wrapper = _jobBussiness.GetAllJobsSearchList(0, "", "", DateTime.MinValue, DateTime.MinValue, Convert.ToBoolean(isReturn)); // default values as no criteria required
            return wrapper;
        }

        [Route("jobissuesave")]
        [HttpPost]
        public ActionResult<TransactionWrapper> JobIssueSave([FromBody] JobIssueModel issueJob)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            if (issueJob.IssueQty == 0)
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("JobIssueSave: Issue Quantity cannot be 0.");
                return wrapper;
            }
            else if (issueJob.IssueQty > issueJob.AvailableQty)
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("JobIssueSave: Issue Quantity cannot be greater than available qty.");
                return wrapper;
            }
            /*else if (issueJob.IssueQty > issueJob.ReqQty)
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("JobIssueSave: Issue Quantity cannot be greater than movt qty.");
                return wrapper;
            }*/
            else
            {
                wrapper = _jobBussiness.JobIssueSave(issueJob);

                try
                {
                    string logFileName = String.Format("JobIssueSave_Wrapper_{0}_{1}.txt", DateTime.Now.ToString("yyyyMMdd"), issueJob.JobNo.ToString());

                    foreach (string log in wrapper.Messages)
                    {
                        Common.WriteLogFile.WriteLog(logFileName, String.Format("{0} - {1}", DateTime.Now.ToString(), log));
                    }
                }
                catch { }

                return wrapper;
            }
        }
    }
}