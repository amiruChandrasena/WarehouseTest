using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Transactions;
using Abstractions.ServiceInterfaces;
using Business;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Models;

namespace WarehouseScannerApi.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class IssueController : ControllerBase
    {
        private readonly IConfiguration _configuration;
        private readonly IIssueService _issueService;
        private readonly IRawMaterialService _rawMaterialService;
        private readonly IPalletService _palletService;
        private readonly IJobService _jobService;
        private readonly IStockService _stockService;
        private readonly IssueBussiness _issueBussiness;

        public IssueController(IConfiguration configuration, 
            IIssueService issueService, 
            IRawMaterialService rawMaterialService, 
            IPalletService palletService, 
            IJobService jobService, 
            IStockService stockService)
        {
            _configuration = configuration;
            _issueService = issueService;
            _rawMaterialService = rawMaterialService;
            _palletService = palletService;
            _jobService = jobService;
            _stockService = stockService;

            _issueBussiness = new IssueBussiness(_configuration, 
                _issueService, 
                _rawMaterialService, 
                _palletService, 
                _jobService, 
                _stockService);
        }

        [Route("issuepallet")]
        [HttpPost]
        public ActionResult<TransactionWrapper> IssuePallet(IssuePalletRMModel issuePallet)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            if (issuePallet.IssueQty == 0)
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("IssuePallet: Issue Quantity cannot be 0.");
                return wrapper;
            }
            else if (issuePallet.IssueQty > issuePallet.AvailableQty)
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("IssuePallet: Issue qty greater than Pallet qty.");
                return wrapper;
            }
            else
            {
                wrapper = _issueBussiness.IssuePallet(issuePallet);
                return wrapper;
            }
        }

        [Route("issue")]
        [HttpPost]
        public ActionResult<TransactionWrapper> Issue(IssueRMModel issue)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            try
            {
                if (issue.IssueQty == 0)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("IssuePallet: Issue quantity cannot be 0.");
                    return wrapper;
                }
                else if (issue.IssueQty > issue.AvailableQty)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("IssuePallet: Issue qty greater than pallet qty.");
                    return wrapper;
                }
                else
                {
                    wrapper = _issueBussiness.Issue(issue);
                    return wrapper;
                }
            } catch (Exception e)
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add(e.Message);
                return wrapper;
            }
        }
    }
}