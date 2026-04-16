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
    public class StockTransferController : ControllerBase
    {
        private readonly IStockTransferService _stockTransferService;
        private readonly IRawMaterialService _rawMaterialService;
        private readonly IPalletService _palletService;
        private readonly StockTransferBusiness _stockTransferBussiness;

        public StockTransferController(IStockTransferService stockTransferService, IRawMaterialService rawMaterialService, IPalletService palletService)
        {
            _stockTransferService = stockTransferService;
            _rawMaterialService = rawMaterialService;
            _palletService = palletService;
            _stockTransferBussiness = new StockTransferBusiness(_stockTransferService, _rawMaterialService, _palletService);
        }

        [Route("getrmstocktransferdetails/{transferNo}")]
        [HttpGet]
        public ActionResult<TransactionWrapper> GetRMStockTransferByTransNo(string transferNo)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            if (String.IsNullOrEmpty(transferNo))
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("GetRMStockTransferByTransNo: Fail due to missing transferNo");
                return wrapper;
            }
            else
            {
                wrapper = _stockTransferBussiness.GetRMStockTransferByTransferNo(transferNo);
                return wrapper;
            }
        }

        [Route("gettransitwarehouse")]
        [HttpGet]
        public ActionResult<TransactionWrapper> GetTransitWarehouse()
        {
            return _stockTransferBussiness.GetTransitWarehouse();
        }

        [Route("getallrmstocktransferheaderlist/{originator}/{defaultwarehouse}")]
        [HttpGet]
        public ActionResult<TransactionWrapper> GetAllRMStockTransferHeaderList(string originator, string defaultwarehouse)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            if (String.IsNullOrEmpty(originator))
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("GetAllRMStockTransferHeaderList: Fail due to missing originator");
                return wrapper;
            }
            else if (String.IsNullOrEmpty(defaultwarehouse))
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("GetAllRMStockTransferHeaderList: Fail due to missing from warehouse");
                return wrapper;
            }
            else
            {
                wrapper = _stockTransferBussiness.GetAllRMStockTransferHeaderList(originator, defaultwarehouse);
                return wrapper;
            }
        }

        [Route("issuetransfer")]
        [HttpPost]
        public ActionResult<TransactionWrapper> IssueTransfer([FromBody] IssueTransferRMModel issueTrans)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            
            if (issueTrans.IssueQty == 0)
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("IssueTransfer: Issue Quantity cannot be 0.");
                return wrapper;
            }
            else if (issueTrans.IssueQty > issueTrans.AvailableQty)
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("IssueTransfer: Issue Quantity cannot be greater than available qty.");
                return wrapper;
            }
            else if (issueTrans.IssueQty > issueTrans.ReqQty)
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("IssueTransfer: Issue Quantity cannot be greater than req. qty.");
                return wrapper;
            }
            else
            {
                wrapper = _stockTransferBussiness.IssueTransferRM(issueTrans);
                return wrapper;
            }
        }

        [Route("finalizetransfer")]
        [HttpPost]
        public ActionResult<TransactionWrapper> FinalizeTransfer([FromBody] StockTransferRMHeaderModel finalizeTrans)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            if (finalizeTrans.TransferNo == 0)
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("FinalizeTransfer: Please enter the transfer no first");
                return wrapper;
            }
            else if (finalizeTrans.OpenPalletNo > 0)
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("FinalizeTransfer: Close current mix pallet.");
                return wrapper;
            }
            else
            {
                foreach (var obj in finalizeTrans.StockTransferDetails)
                {
                    if (obj.IssueQty < obj.MoveQty)
                    {
                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add("FinalizeTransfer: Raw materials must be fully issued to finalise this transfer");
                        return wrapper;
                    }
                }

                wrapper = _stockTransferBussiness.FinalizeTransfer(finalizeTrans);
                return wrapper;
            }
        }
    }
}