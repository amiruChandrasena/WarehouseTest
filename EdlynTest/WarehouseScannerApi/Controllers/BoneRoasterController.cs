using Abstractions.ServiceInterfaces;
using Business;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace WarehouseScannerApi.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class BoneRoasterController : ControllerBase
    {
        private readonly BoneRoasterBussiness _boneRoasterBusiness;
        private readonly IBoneRoasterService _boneRoasterService;

        public BoneRoasterController(IBoneRoasterService boneRoasterService)
        {
            _boneRoasterService = boneRoasterService;
            _boneRoasterBusiness = new BoneRoasterBussiness(_boneRoasterService);
        }

        #region Transfer Pallet to Transit

        //Load Pallet Details for Warehouse B2
        [Route("GetAllPalletDetails/{warehouseCode}")]
        [HttpGet]
        public ActionResult<TransactionWrapper> GetAllPalletDetails(string warehouseCode="B2")
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            if (String.IsNullOrEmpty(warehouseCode))
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("GetAllPalletDetails : Warehouse Code is empty");
                return wrapper;
            }
            else
            {
                wrapper = _boneRoasterBusiness.GetAllPalletDetails(warehouseCode);
                return wrapper;
            }
        }

        [Route("GetTodayManifestNumber")]
        [HttpGet]
        public ActionResult<TransactionWrapper> GetTodayManifestNumber()
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            wrapper = _boneRoasterBusiness.GetTodayManifestNumber();
            return wrapper;
        }

        [Route("TransferPallet")]
        [HttpPost]
        public ActionResult<TransactionWrapper> TransferPallet(TransferPalletBRModel transferPallet)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            //if (String.IsNullOrEmpty(warehouseCode))
            //{
            //    wrapper.IsSuccess = false;
            //    wrapper.Messages.Add("GetAllPalletDetails : Warehouse Code is empty");
            //    return wrapper;
            //}
            //else
            //{
                wrapper = _boneRoasterBusiness.TransferPallet(transferPallet);
                return wrapper;
            //}
        }

        #endregion

        #region Receive Pallet

        //API for Load Manifest details for BoneRoaster By Warehouse 'B7'
        [Route("GetAllManifestsByWarehouse/{warehouseCode}")]
        [HttpGet]
        public ActionResult<TransactionWrapper> GetAllManifestsByWarehouse(string warehouseCode="B7")
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            if (String.IsNullOrEmpty(warehouseCode))
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("GetAllManifestsByWarehouse : Warehouse Code is empty");
                return wrapper;
            }
            else
            {
                wrapper = _boneRoasterBusiness.GetAllManifestsByWarehouse(warehouseCode);
                return wrapper;
            }
        }

        //API for Load Manifest details for BoneRoaster By Warehouse 'B7'
        [Route("GetManifestDetailsByManifestNo/{manifestNo}")]
        [HttpGet]
        public ActionResult<TransactionWrapper> GetManifestDetailsByManifestNo(string manifestNo = "")
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            if (String.IsNullOrEmpty(manifestNo))
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("GetManifestDetailsByManifestNo : Manifest number is empty");
                return wrapper;
            }
            else
            {
                wrapper = _boneRoasterBusiness.GetManifestDetailsByManifestNo(manifestNo);
                return wrapper;
            }
        }

        [Route("ReceivePallet")]
        [HttpPost]
        public ActionResult<TransactionWrapper> ReceivePallet(ReceivePalletBRModel transferPallet)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            //if (String.IsNullOrEmpty(warehouseCode))
            //{
            //    wrapper.IsSuccess = false;
            //    wrapper.Messages.Add("GetAllPalletDetails : Warehouse Code is empty");
            //    return wrapper;
            //}
            //else
            //{
            wrapper = _boneRoasterBusiness.ReceivePallet(transferPallet);
            return wrapper;
            //}
        }

        #endregion
    }
}
