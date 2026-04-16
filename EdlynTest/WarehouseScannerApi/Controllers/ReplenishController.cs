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
    public class ReplenishController : ControllerBase
    {
        private readonly ReplenishBusiness _replenishBusiness;
        private readonly IReplenishService _replenishService;

        public ReplenishController(IReplenishService replenishService)
        {
            _replenishService = replenishService;
            _replenishBusiness = new ReplenishBusiness(_replenishService);
        }

        [Route("getbinlocations/{warehouseCode}/{roomCode}/{isReplenish}/{isPullDown}")]
        [HttpGet]
        public ActionResult<TransactionWrapper> GetBinLocations(string warehouseCode, string roomCode, bool isReplenish, bool isPullDown)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            if (String.IsNullOrEmpty(warehouseCode) || String.IsNullOrEmpty(roomCode))
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("GetBinLocations : Empty string value(s)");
                return wrapper;
            }
            else
            {
                wrapper = _replenishBusiness.GetBinLocations(warehouseCode, roomCode, isReplenish, isPullDown);
                return wrapper;
            }
        }

        [Route("getbinlocations-hotlist/{warehouseCode}/{roomCode}/{isReplenish}/{isPullDown}")]
        [HttpGet]
        public ActionResult<TransactionWrapper> GetBinLocationsHotList(string warehouseCode, string roomCode, bool isReplenish, bool isPullDown)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            if (String.IsNullOrEmpty(warehouseCode) || String.IsNullOrEmpty(roomCode))
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("GetBinLocations : Empty string value(s)");
                return wrapper;
            }
            else
            {
                wrapper = _replenishBusiness.GetBinLocations(warehouseCode, roomCode, isReplenish, isPullDown);
                return wrapper;
            }
        }

        [Route("getnextsuggestedrack/{warehouseCode}/{roomCode}/{catalogCode}/{bestBefore}/{pickingSequence}")]
        [HttpGet]
        public ActionResult<TransactionWrapper> GetNextSuggestedRack(string warehouseCode, string roomCode, string catalogCode, string bestBefore, int pickingSequence)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            if (String.IsNullOrEmpty(warehouseCode) || String.IsNullOrEmpty(roomCode) || String.IsNullOrEmpty(catalogCode) || String.IsNullOrEmpty(bestBefore))
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("GetNextSuggestedRack : Empty string value(s)");
                return wrapper;
            }
            /*
            if (bestBefore == DateTime.MinValue || bestBefore == DateTime.MaxValue)
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("GetNextSuggestedRack : Invalid best before date");
                return wrapper;
            }*/

            wrapper = _replenishBusiness.GetNextSuggestedRack(warehouseCode, roomCode, catalogCode, bestBefore, pickingSequence);
            return wrapper;
        }

        [Route("getprevioussuggestedrack/{warehouseCode}/{roomCode}/{catalogCode}/{bestBefore}/{pickingSequence}")]
        [HttpGet]
        public ActionResult<TransactionWrapper> GetPreviousSuggestedRack(string warehouseCode, string roomCode, string catalogCode, string bestBefore, int pickingSequence)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            if (String.IsNullOrEmpty(warehouseCode) || String.IsNullOrEmpty(roomCode) || String.IsNullOrEmpty(catalogCode) || String.IsNullOrEmpty(bestBefore))
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("GetPreviousSuggestedRack : Empty string value(s)");
                return wrapper;
            }

            wrapper = _replenishBusiness.GetPreviousSuggestedRack(warehouseCode, roomCode, catalogCode, bestBefore, pickingSequence);
            return wrapper;
        }

        [Route("getreplenishitemdetails/{catalogCode}/{warehouseCode}/{roomCode}")]
        [HttpGet]
        public ActionResult<TransactionWrapper> GetReplenishItemDetails(string catalogCode, string warehouseCode, string roomCode)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            if (String.IsNullOrEmpty(catalogCode) || String.IsNullOrEmpty(warehouseCode) || String.IsNullOrEmpty(roomCode))
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("GetReplenishItemDetails : Empty string value(s)");
                return wrapper;
            }
            else
            {
                wrapper = _replenishBusiness.GetReplenishItemDetails(catalogCode, warehouseCode, roomCode);
                return wrapper;
            }
        }

        [Route("getsuggestedrack/{binLocation}")]
        [HttpGet]
        public ActionResult<TransactionWrapper> GetSuggestedRack(string binLocation)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            if (String.IsNullOrEmpty(binLocation))
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("GetSuggestedRack : Empty bin location string");
                return wrapper;
            }
            else
            {
                wrapper = _replenishBusiness.GetSuggestedRack(binLocation);
                return wrapper;
            }
        }

        [Route("processrack")]
        [HttpPost]
        public ActionResult<TransactionWrapper> ProcessRack(MoveToRackDto moveToRackDto)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            if (moveToRackDto.Pallets == null)
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("Pallets to move are empty");
                return wrapper;
            }
            else if (moveToRackDto.Pallets.Count == 0)
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("Pallets to move are empty");
                return wrapper;
            }
            else if (String.IsNullOrEmpty(moveToRackDto.WarehouseCode) || String.IsNullOrEmpty(moveToRackDto.RoomCode) || String.IsNullOrEmpty(moveToRackDto.RackCode)
                        || String.IsNullOrEmpty(moveToRackDto.Originator))
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("One or more required components is blank");
                return wrapper;
            }

            wrapper = _replenishBusiness.ProcessRack(moveToRackDto.WarehouseCode, moveToRackDto.RoomCode, moveToRackDto.RackCode, moveToRackDto.Originator, moveToRackDto.Pallets[0]);
            return wrapper;
        }

        [Route("validatepallet")]
        [HttpPost]
        public ActionResult<TransactionWrapper> ValidatePallet([FromBody] PalletValidationModel palletValidation)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            if (palletValidation != null)
            {
                if (palletValidation.PalletNumbers.Length == 0 || String.IsNullOrEmpty(palletValidation.Originator)||
                    String.IsNullOrEmpty(palletValidation.WarehouseCode)|| String.IsNullOrEmpty(palletValidation.RoomCode))
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("Error: One or or more passed values are empty");
                    return wrapper;
                }
            }

            wrapper = _replenishBusiness.ValidatePallet(palletValidation.PalletNumbers[0], 
                                                        palletValidation.IsPulldown, 
                                                        palletValidation.IsReplenish, 
                                                        palletValidation.Originator, 
                                                        palletValidation.WarehouseCode,
                                                        palletValidation.RoomCode);
            return wrapper;
        }
    }
}