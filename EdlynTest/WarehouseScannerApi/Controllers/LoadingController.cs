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
    public class LoadingController : ControllerBase
    {
        private readonly LoadingBusiness _loadingBusiness;
        private readonly ILoadingService _loadingService;

        public LoadingController(ILoadingService loadingService)
        {
            _loadingService = loadingService;
            _loadingBusiness = new LoadingBusiness(_loadingService);
        }

        [Route("getdeliverydetails/{customerCode}/{catalogCode}/{assigneeNumber}")]
        [HttpGet]
        public ActionResult<TransactionWrapper> GetDeliveryDetails(string customerCode, string catalogCode, int assigneeNumber)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            if (String.IsNullOrEmpty(customerCode) || String.IsNullOrEmpty(catalogCode))
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("GetDeliveryDetails : Empty Customer or Catalog code");
                return wrapper;
            }
            else
            {
                wrapper = _loadingService.GetDeliveryDetails(customerCode, catalogCode, assigneeNumber);
                return wrapper;
            }
        }

        [Route("getpalletdetails/{palletNumber}")]
        [HttpGet]
        public ActionResult<TransactionWrapper> GetPalletDetails(int palletNumber)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            if (palletNumber == 0)
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("GetPalletDetails : No Pallet numbers");
                return wrapper;
            }
            else
            {
                wrapper = _loadingService.GetPalletDetails(palletNumber);
                return wrapper;
            }
        }

        [Route("getpalletsinmanifest/{manifestNumber}")]
        [HttpGet]
        public ActionResult<TransactionWrapper> GetPalletsInManifest(int manifestNumber)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            if (manifestNumber == 0)
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("GetPalletsInManifest : Manifest number is 0");
                return wrapper;
            }
            else
            {
                wrapper = _loadingService.GetPalletsInManifest(manifestNumber);
                return wrapper;
            }
        }

        [Route("getpicklistnumber/{invoiceNumber}")]
        [HttpGet]
        public ActionResult<TransactionWrapper> GetPicklistNumber(int invoiceNumber)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            if (invoiceNumber == 0)
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("GetPicklistNumber : Invoice number is 0");
                return wrapper;
            }
            else
            {
                wrapper = _loadingBusiness.GetPicklistNumber(invoiceNumber);
                return wrapper;
            }
        }

        [Route("updatepalletdetaildespatched")]
        [HttpPost]
        public ActionResult<TransactionWrapper> UpdatePalletDetailDespatched([FromBody] int palletNumber)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            if (palletNumber == 0)
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("UpdatePalletDetailDespatched : Pallet number is 0");
                return wrapper;
            }
            else
            {
                wrapper = _loadingBusiness.UpdatePalletDetailDespatched(palletNumber);
                return wrapper;
            }
        }
    }
}