using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Abstractions.ServiceInterfaces;
using Business;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Models;
using Models.Dto;

namespace WarehouseScannerApi.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class PickingController : ControllerBase
    {
        private readonly PickingBusiness _pickingBusiness;
        private readonly IPickingService _pickingService;
        private readonly IPalletService _palletService;

        public PickingController(IPickingService pickingService, IPalletService palletService)
        {
            _pickingService = pickingService;
            _palletService = palletService;
            _pickingBusiness = new PickingBusiness(_pickingService, _palletService);
        }

        [Route("checkit/{pickingLabel}")]
        [HttpGet]
        public ActionResult<TransactionWrapper> CheckIt(string pickingLabel)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            if (String.IsNullOrEmpty(pickingLabel))
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("Empty picking label");
                return wrapper;
            }

            wrapper = _pickingBusiness.CheckIt(pickingLabel);
            return wrapper;
        }

        [Route("closepallet")]
        [HttpPost]
        public ActionResult<TransactionWrapper> ClosePallet(PickingDto pickingDto)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            if (String.IsNullOrEmpty(pickingDto.Originator))
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("ClosePallet : Originator is empty");
                return wrapper;
            } 
            else if (pickingDto.PalletNumber == 0 || pickingDto.ManifestNumber == 0 || pickingDto.PicklistNumber == 0)
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("ClosePallet : Some numeric values are 0");
                return wrapper;
            }

            wrapper = _pickingBusiness.ClosePallet(pickingDto.PalletNumber, 
                pickingDto.Originator, 
                pickingDto.ManifestNumber, 
                pickingDto.PicklistNumber,
                pickingDto.PalletCount,
                pickingDto.PalletSpaces);

            return wrapper;
        }

        [Route("closepickingscreen")]
        [HttpPost]
        public ActionResult<TransactionWrapper> ClosePickingScreen(int manifestNo, int picklistNo, string originator)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            if (String.IsNullOrEmpty(originator))
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("ClosePickingScreen : Originator is empty");
                return wrapper;
            }
            else if (manifestNo == 0 || picklistNo == 0)
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("Manifest Number or Picklist Number is 0");
                return wrapper;
            }
            else
            {
                wrapper = _pickingBusiness.ClosePickingScreen(manifestNo, picklistNo, originator);
                return wrapper;
            }
        }
        
        [Route("confirmpick")]
        [HttpPost]
        public ActionResult<TransactionWrapper> ConfirmPick(PickingDto pickingDto)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            if (pickingDto.CatalogItem == null || pickingDto.PicklistItems == null || String.IsNullOrEmpty(pickingDto.Originator) || pickingDto.PickingQuantity == 0
                || pickingDto.PalletNumber == 0 || pickingDto.PicklistNumber == 0 || pickingDto.ManifestNumber == 0)
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("Error receiving data");
                return wrapper;
            }

            wrapper = _pickingBusiness.ConfirmPick(pickingDto.CatalogItem, pickingDto.PickedItems, pickingDto.PicklistItems, pickingDto.NoNegativePickBin, pickingDto.PickingFromPickPhase,
                                                   pickingDto.PickingQuantity, pickingDto.PalletNumber, pickingDto.PickingPartOfPalletNumber, pickingDto.CatalogItem.CatalogCode, pickingDto.PickingPartOfPallet,
                                                   pickingDto.PicklistNumber, pickingDto.ManifestNumber, pickingDto.Originator, pickingDto.BinLocation);
            return wrapper;
        }


        [Route("getpickingitems/{originator}/{warehouseCode}/{roomCode}/{isTransfer}")]
        [HttpGet]
        public ActionResult<TransactionWrapper> GetPickingItems(string originator, string warehouseCode, string roomCode, bool isTransfer)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            if (String.IsNullOrEmpty(originator) || String.IsNullOrEmpty(warehouseCode))
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("GetPickingItems : String values are empty");
                return wrapper;
            }

            wrapper = _pickingBusiness.GetPickingItems(originator, warehouseCode, roomCode, isTransfer);
            return wrapper;
        }

        [Route("getcurrentpicklist/{originator}/{picklistNo}/{manifestNo}/{warehouseCode}/{roomCode}")]
        [HttpGet]
        public ActionResult<TransactionWrapper> GetCurrentPicklist(string originator, int picklistNo, int manifestNo, string warehouseCode, string roomCode)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            if (picklistNo == 0 || manifestNo == 0)
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("GetCurrentPicklist : PickingNo or ManifestNo is zero");
                return wrapper;
            }

            if (String.IsNullOrEmpty(originator) || String.IsNullOrEmpty(warehouseCode) || String.IsNullOrEmpty(roomCode))
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("GetPickingItems : String values are empty");
                return wrapper;
            }

            wrapper = _pickingBusiness.GetCurrentPicklist(originator, picklistNo, manifestNo, warehouseCode, roomCode);
            return wrapper;
        }

        [Route("finalisepicklist")]
        [HttpPost]
        public ActionResult<TransactionWrapper> FinalisePicklist(PickingDto pickingDto)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            int picklistItemsRem = 0;

            if (pickingDto.PicklistItems != null)
            {
                if (pickingDto.PicklistItems.Count() > 0)
                {
                    picklistItemsRem = pickingDto.PicklistItems.Count();
                }
            }

            if (String.IsNullOrEmpty(pickingDto.WarehouseCode) || String.IsNullOrEmpty(pickingDto.Originator) || String.IsNullOrEmpty(pickingDto.IsTransfer))
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("One or more string values are empty");
                return wrapper;
            }
            else if (pickingDto.ManifestNumber == 0)
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("Manifest number is zero");
                return wrapper;
            }
            else
            {
                wrapper = _pickingBusiness.FinalisePicklist(pickingDto.WarehouseCode, pickingDto.Originator, pickingDto.IsTransfer, pickingDto.ManifestNumber,
                                                            pickingDto.PicklistNumber, picklistItemsRem, pickingDto.PalletCount, pickingDto.PalletSpaces, pickingDto.PickedItems);
                return wrapper;
            }
        }

        [Route("getnegativepickingbin")]
        [HttpGet]
        public ActionResult<TransactionWrapper> GetNegativePickBin()
        {
            TransactionWrapper wrapper = _pickingBusiness.GetNegativePickBin();
            return wrapper;
        }

        [Route("opennewpallet")]
        [HttpPost]
        public ActionResult<TransactionWrapper> OpenNewPallet(PickingDto pickingDto)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            if (String.IsNullOrEmpty(pickingDto.Originator) || String.IsNullOrEmpty(pickingDto.WarehouseCode) || pickingDto.PicklistNumber == 0 || pickingDto.ManifestNumber == 0)
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("OpenNewPallet : Some values are invalid");
                return wrapper;
            }

            wrapper = _pickingBusiness.OpenNewPallet(pickingDto.Originator, pickingDto.WarehouseCode, pickingDto.ManifestNumber, pickingDto.PicklistNumber);
            return wrapper;
        }

        [Route("scanlabel")]
        [HttpPost]
        public ActionResult<TransactionWrapper> ScanLabel(PickingDto pickingDto)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            
            if (String.IsNullOrEmpty(pickingDto.ScanData) || String.IsNullOrEmpty(pickingDto.WarehouseCode) || String.IsNullOrEmpty(pickingDto.RoomCode))
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("One or more string values are empty");
                return wrapper;
            }

            wrapper = _pickingBusiness.ScanLabel(pickingDto.ScanData, pickingDto.WarehouseCode, pickingDto.RoomCode, pickingDto.PicklistNumber);
            return wrapper;
        }

        [Route("scanpickinglabel")]
        [HttpPost]
        public ActionResult<TransactionWrapper> ScanPickingLabel(PickingDto pickingDto)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            if (String.IsNullOrEmpty(pickingDto.Originator) || String.IsNullOrEmpty(pickingDto.PickingLabel))
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("One or more string values are empty");
                return wrapper;
            }
            if (pickingDto.PalletNumber == 0 || pickingDto.ManifestNumber == 0 || pickingDto.PicklistNumber == 0)
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("One or more integer values are empty");
                return wrapper;
            }

            wrapper = _pickingBusiness.ScanPickingLabel(pickingDto.PickingLabel, pickingDto.Originator, pickingDto.PalletNumber, pickingDto.ManifestNumber, pickingDto.PicklistNumber);
            return wrapper;
        }

        [Route("removepickeditem")]
        [HttpPost]
        public ActionResult<TransactionWrapper> RemovePickedItem(PickingDto pickingDto)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            wrapper = _pickingBusiness.RemovePicked(pickingDto.ManifestLoadStatus, pickingDto.WarehouseCode, pickingDto.RoomCode, pickingDto.Originator);
            return wrapper;
        }

        [Route("pickwholepallet")]
        [HttpPost]
        public ActionResult<TransactionWrapper> PickWholePallet(PickingDto pickingDto)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            if (pickingDto != null)
            {
                wrapper = _pickingBusiness.ConfirmItemsInPicklist(pickingDto.ManifestLoadStatus, pickingDto.PalletQuantity, pickingDto.PicklistNumber, pickingDto.ManifestNumber,
                                                                  pickingDto.Originator, pickingDto.BinLocation, pickingDto.PalletCount, pickingDto.PalletSpaces);
                return wrapper;
            } else
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("null object in call");
                return wrapper;
            }
        }

        [HttpGet("gettransitwarehouses")]
        public ActionResult<TransactionWrapper> GetTransitWarehouses([FromQuery] string fromWh = null, [FromQuery] string toWh = null)
        {
            var wrapper = _pickingBusiness.GetTransitWarehouses(fromWh, toWh);
            return wrapper;
        }
    }
}