using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Abstractions.ServiceInterfaces;
using Business;
using Models;

namespace WarehouseScannerApi.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class PutAwayController : ControllerBase
    {
        private readonly PutAwayBusiness _putAwayBusiness;
        private readonly IPutAwayService _putAwayService;

        public PutAwayController(IPutAwayService putAwayService)
        {
            _putAwayService = putAwayService;
            _putAwayBusiness = new PutAwayBusiness(_putAwayService);
        }

        [Route("checkpalletvalidationrm")]
        [HttpPost]
        public ActionResult<TransactionWrapper> CheckPalletValidationRM(MoveToRackDto moveToRack)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            if (moveToRack.Pallets == null)
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("Pallets to move are null");
                return wrapper;
            }
            else if (moveToRack.Pallets.Count == 0)
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("Pallets to move are empty");
                return wrapper;
            }
            else if (String.IsNullOrEmpty(moveToRack.WarehouseCode) || String.IsNullOrEmpty(moveToRack.RoomCode) || String.IsNullOrEmpty(moveToRack.RackCode)
                        || String.IsNullOrEmpty(moveToRack.Originator))
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("One or more required components is blank");
                return wrapper;
            }

            string scannedBinLocation = moveToRack.WarehouseCode + "." + moveToRack.RoomCode + "." + moveToRack.RackCode;

            wrapper = _putAwayBusiness.CheckValidationProcessRackRM(scannedBinLocation, moveToRack.Originator, moveToRack.Pallets[0].CatalogCode);
            if (wrapper.IsSuccess == false)
            {
                return wrapper;
            }

            if (wrapper.ResultSet.Count > 0)
            {
                return wrapper;
            }
            else
            {
                wrapper = _putAwayBusiness.EndProcessRackRawMaterials(moveToRack.Pallets[0], moveToRack.Originator, scannedBinLocation);
                return wrapper;
            }
        }

        [Route("consolidatepallets")]
        [HttpPost]
        public ActionResult<TransactionWrapper> ConsolidatePallets(MoveToRackDto moveToRack)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            if (moveToRack.Pallets == null)
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("Pallets to move are null");
                return wrapper;
            }
            else if (moveToRack.Pallets.Count == 0)
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("Pallets to move are empty");
                return wrapper;
            }
            else if (String.IsNullOrEmpty(moveToRack.WarehouseCode) || String.IsNullOrEmpty(moveToRack.RoomCode) || String.IsNullOrEmpty(moveToRack.RackCode)
                        || String.IsNullOrEmpty(moveToRack.Originator))
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("One or more required components is blank");
                return wrapper;
            }

            string scannedBinLocation = moveToRack.WarehouseCode + "." + moveToRack.RoomCode + "." + moveToRack.RackCode;
            wrapper = _putAwayBusiness.ProcessRackRMConsolidate(moveToRack.Pallets[0], scannedBinLocation, moveToRack.Originator);
            return wrapper;
        }

        [Route("beginprocessrackrm")]
        [HttpPost]
        public ActionResult<TransactionWrapper> BeginProcessRackRM(MoveToRackDto moveToRack)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            if (moveToRack.Pallets == null)
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("Pallets to move are null");
                return wrapper;
            }
            else if (moveToRack.Pallets.Count == 0)
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("Pallets to move are empty");
                return wrapper;
            }
            else if (String.IsNullOrEmpty(moveToRack.WarehouseCode) || String.IsNullOrEmpty(moveToRack.RoomCode) || String.IsNullOrEmpty(moveToRack.RackCode)
                        || String.IsNullOrEmpty(moveToRack.Originator))
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("One or more required components is blank");
                return wrapper;
            }

            string scannedBinLocation = moveToRack.WarehouseCode + "." + moveToRack.RoomCode + "." + moveToRack.RackCode;
            wrapper = _putAwayBusiness.BeginProcessRackRawMaterials(moveToRack.WarehouseCode, moveToRack.RoomCode, moveToRack.RackCode, moveToRack.Originator, moveToRack.Pallets[0]);
            return wrapper;
        }

        [Route("endprocessrackrm")]
        [HttpPost]
        public ActionResult<TransactionWrapper> EndProcessRackRM(MoveToRackDto moveToRack)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            if (moveToRack.Pallets == null)
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("Pallets to move are null");
                return wrapper;
            }
            else if (moveToRack.Pallets.Count == 0)
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("Pallets to move are empty");
                return wrapper;
            }
            else if (String.IsNullOrEmpty(moveToRack.WarehouseCode) || String.IsNullOrEmpty(moveToRack.RoomCode) || String.IsNullOrEmpty(moveToRack.RackCode)
                        || String.IsNullOrEmpty(moveToRack.Originator))
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("One or more required components is blank");
                return wrapper;
            }

            string scannedBinLocation = moveToRack.WarehouseCode + "." + moveToRack.RoomCode + "." + moveToRack.RackCode;
            wrapper = _putAwayBusiness.EndProcessRackRawMaterials(moveToRack.Pallets[0], moveToRack.Originator, scannedBinLocation);
            return wrapper;
        }

        [Route("getpalletnumbersinrack/{binLocation}")]
        [HttpGet]
        public ActionResult<TransactionWrapper> GetPalletNumbersInRack(string binLocation)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            if (String.IsNullOrEmpty(binLocation))
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("GetPalletNumbersInRack: Fail due to missing bin location section");
                return wrapper;
            }
            else
            {
                wrapper = _putAwayBusiness.GetPalletNumbersInRack(binLocation);
                return wrapper;
            }
        }

        [Route("getpalletstatus")]
        [HttpGet]
        public ActionResult<TransactionWrapper> GetPalletStatus([FromQuery] int[] palletNumbers)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            if (palletNumbers.Length == 0)
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("No pallet numbers to check");
                return wrapper;
            }
            else
            {
                wrapper = _putAwayBusiness.GetPalletStatus(palletNumbers);
                return wrapper;
            }
        }

        [Route("processrack")]
        [HttpPost]
        public ActionResult<TransactionWrapper> ProcessRack(MoveToRackDto moveToRack)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            if (moveToRack.Pallets == null)
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("Pallets to move are empty");
                return wrapper;
            }
            else if (moveToRack.Pallets.Count == 0)
            {

            }
            else if (String.IsNullOrEmpty(moveToRack.WarehouseCode) || String.IsNullOrEmpty(moveToRack.RoomCode) || String.IsNullOrEmpty(moveToRack.RackCode)
                        || String.IsNullOrEmpty(moveToRack.Originator))
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("One or more required components is blank");
                return wrapper;
            }

            wrapper = _putAwayBusiness.ProcessRack(moveToRack.WarehouseCode, moveToRack.RoomCode, moveToRack.RackCode, moveToRack.Originator, moveToRack.Pallets);
            return wrapper;
        }

        [Route("validatepallets")]
        [HttpPost]
        public ActionResult<TransactionWrapper> ValidatePallets([FromBody] PalletValidationModel palletValidation)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            if (palletValidation != null)
            {
                if (palletValidation.PalletNumbers.Length == 0 || String.IsNullOrEmpty(palletValidation.Originator) ||
                    String.IsNullOrEmpty(palletValidation.WarehouseCode) || String.IsNullOrEmpty(palletValidation.RoomCode))
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("Error: One or more passed values are empty");
                    return wrapper;
                }
            }
            else
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("ValidatePallets: Empty pallet validation object");
                return wrapper;
            }

            wrapper = _putAwayBusiness.ValidatePallets(palletValidation.PalletNumbers,
                palletValidation.Originator,
                palletValidation.WarehouseCode,
                palletValidation.RoomCode,
                palletValidation.IsReplenish);

            return wrapper;
        }

        [Route("validatepalletrm")]
        [HttpPost]
        public ActionResult<TransactionWrapper> ValidatePalletRM([FromBody] PalletValidationModel palletValidation)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            if (palletValidation != null)
            {
                if (String.IsNullOrEmpty(palletValidation.ScanData) || String.IsNullOrEmpty(palletValidation.Originator) ||
                    String.IsNullOrEmpty(palletValidation.WarehouseCode) || String.IsNullOrEmpty(palletValidation.RoomCode))
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("Error: One or more passed values are empty");
                    return wrapper;
                }
            }
            else
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("ValidatePallets: Empty pallet validation object");
                return wrapper;
            }

            wrapper = _putAwayBusiness.ValidatePalletRawMaterials(palletValidation.ScanData,
                palletValidation.Originator,
                palletValidation.WarehouseCode,
                palletValidation.RoomCode);

            return wrapper;
        }

        [Route("scanmixpallet")]
        [HttpPost]
        public ActionResult<TransactionWrapper> ScanMixPallet([FromBody] PalletMixModel palletMix)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            if (palletMix != null)
            {
                if (palletMix.palletDetail.PalletUnits == 0)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("ScanMixPallet: Zero units selected!");
                    return wrapper;
                }
                else if (String.IsNullOrEmpty(palletMix.BinLocationTo))
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add($"ScanMixPallet: No rack found {palletMix.BinLocationTo}");
                    return wrapper;
                }
            }
            else
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("ScanMixPallet: Empty pallet validation object");
                return wrapper;
            }

            wrapper = _putAwayBusiness.ScanMixPallet(palletMix);

            return wrapper;
        }

        [Route("validatemixpallet")]
        [HttpPost]
        public ActionResult<TransactionWrapper> ValidateMixPallet([FromBody] PalletMixModel palletMix)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            if (palletMix != null)
            {
                if (String.IsNullOrEmpty(palletMix.BinLocationTo))
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add($"ValidateMixPallet: No rack found {palletMix.BinLocationTo}");
                    return wrapper;
                }
            }
            else
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("ValidateMixPallet: Empty pallet validation object");
                return wrapper;
            }

            wrapper = _putAwayBusiness.ValidateMixPallet(palletMix);

            return wrapper;
        }

        [Route("validatebulkmixpallet")]
        [HttpPost]
        public ActionResult<TransactionWrapper> ValidateBulkMixPallet([FromBody] PalletMixModel palletMix)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            if (palletMix != null)
            {
                if (String.IsNullOrEmpty(palletMix.BinLocationTo))
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add($"ValidateBulkMixPallet: No rack found {palletMix.BinLocationTo}");
                    return wrapper;
                }
            }
            else
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("ValidateBulkMixPallet: Empty pallet validation object");
                return wrapper;
            }

            wrapper = _putAwayBusiness.ValidateBulkMixPallet(palletMix);

            return wrapper;
        }
    }
}