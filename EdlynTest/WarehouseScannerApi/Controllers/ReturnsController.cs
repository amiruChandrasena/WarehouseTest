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
    public class ReturnsController : ControllerBase
    {
        private readonly IRawMaterialService _rawMaterialService;
        private readonly IJobService _jobService;
        private readonly IPalletService _palletService;
        private readonly ICatalogService _catalogService;
        private readonly IGLAccountService _glAccountService;
        private readonly IStockService _stockService;
        private readonly ReturnsBussiness _returnBussiness;

        public ReturnsController(IJobService jobService, IRawMaterialService rawMaterialService, IPalletService palletService, 
            ICatalogService catalogService, IGLAccountService glAccountService, IStockService stockService)
        {
            _jobService = jobService;
            _rawMaterialService = rawMaterialService;
            _palletService = palletService;
            _catalogService = catalogService;
            _glAccountService = glAccountService;
            _stockService = stockService;
            _returnBussiness = new ReturnsBussiness(_jobService, _rawMaterialService, _palletService, _catalogService, _glAccountService, _stockService);
        }

        [Route("savermreturn")]
        [HttpPost]
        public ActionResult<TransactionWrapper> SaveRMReturn([FromBody] ReturnsModel returns)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            if (returns.JobNo == 0)
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("SaveRMReturns: Job no cannot be 0.");
                return wrapper;
            }
            else if (String.IsNullOrEmpty(returns.CatalogCode))
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("Specify a RM code ");
                return wrapper;
            }
            else if (returns.RetQty <= 0)
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("SaveRMReturns: Return Quantity cannot be 0 or negative");
                return wrapper;
            }
            else
            {
                wrapper = _returnBussiness.SaveRMReturn(returns);
                return wrapper;
            }
        }

        [Route("checktagid")]
        [HttpPost]
        public ActionResult<TransactionWrapper> CheckTagId(ReturnsModel returnsModel)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            if (returnsModel.JobNo == 0)
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("CheckTagId: Job number cannot be 0");
                return wrapper;
            }

            if (String.IsNullOrEmpty(returnsModel.CatalogCode))
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("CheckTagId: Catalog code cannot be empty");
                return wrapper;
            }

            wrapper = _returnBussiness.CheckPalletLabelExist(returnsModel.CatalogCode, returnsModel.JobNo, returnsModel.TagId);
            return wrapper;

        }

        //[Route("savermreturnall")]
        //[HttpPost]
        //public ActionResult<TransactionWrapper> SaveRMReturnAll([FromBody] ReturnsModel returns)
        //{
        //    TransactionWrapper wrapper = new TransactionWrapper();

        //    if (returns.jobNo == 0)
        //    {
        //        wrapper.IsSuccess = false;
        //        wrapper.Messages.Add("SaveRMReturns: Job no cannot be 0.");
        //        return wrapper;
        //    }
        //    else if (returns.catalogCode == "")
        //    {
        //        wrapper.IsSuccess = false;
        //        wrapper.Messages.Add("Specify a RM code ");
        //        return wrapper;
        //    }
        //    else if (returns.retQty > 0)
        //    {
        //        wrapper.IsSuccess = false;
        //        wrapper.Messages.Add("SaveRMReturns: Return Quantity cannot be 0");
        //        return wrapper;
        //    }
        //    else
        //    {
        //        wrapper = _returnBussiness.SaveRMReturn(returns);
        //        return wrapper;
        //    }
        //}
    }
}