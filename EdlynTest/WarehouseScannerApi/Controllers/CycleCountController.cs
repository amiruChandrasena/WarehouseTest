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
    public class CycleCountController : ControllerBase
    {
        private readonly CycleCountBusiness _cycleCountBusiness;
        private readonly ICycleCountService _cycleCountService;

        public CycleCountController(ICycleCountService cycleCountService)
        {
            _cycleCountService = cycleCountService;
            _cycleCountBusiness = new CycleCountBusiness(_cycleCountService);
        }

        [Route("getpalletsonrack/{binLocation}")]
        [HttpGet]
        public ActionResult<TransactionWrapper> GetPalletsOnRack(string binLocation)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            if (String.IsNullOrEmpty(binLocation))
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("GetPalletsOnRack : Empty bin location");
                return wrapper;
            }
            else
            {
                wrapper = _cycleCountBusiness.GetPalletsOnRack(binLocation);
            }

            return wrapper;
        }

        [Route("getrmpalletsonrack/{binLocation}")]
        [HttpGet]
        public ActionResult<TransactionWrapper> GetRMPalletsOnRack(string binLocation)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            if (String.IsNullOrEmpty(binLocation))
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("GetPalletsOnRack : Empty bin location");
                return wrapper;
            }
            else
            {
                wrapper = _cycleCountBusiness.GetRMPalletsInRack(binLocation);
            }

            return wrapper;
        }

        [Route("savepalletsinrack")]
        [HttpPost]
        public ActionResult<TransactionWrapper> SavePalletsInRack(CycleCountDto cycleCountDto)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            if (String.IsNullOrEmpty(cycleCountDto.Originator) || String.IsNullOrEmpty(cycleCountDto.BinLocation))
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("SavePalletsInRack : One or more string values empty");
                return wrapper;
            }
            else
            {
                wrapper = _cycleCountBusiness.SavePalletsInRack(cycleCountDto.BinLocation, cycleCountDto.Originator, cycleCountDto.Pallets);
            }

            return wrapper;
        }

        [Route("savermpalletsinrack")]
        [HttpPost]
        public ActionResult<TransactionWrapper> SaveRMPalletsInRack(CycleCountDto cycleCountDto)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            if (String.IsNullOrEmpty(cycleCountDto.Originator) || String.IsNullOrEmpty(cycleCountDto.BinLocation))
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("SavePalletsInRack : One or more string values empty");
                return wrapper;
            }
            else
            {
                wrapper = _cycleCountBusiness.SaveRMPalletsInRack(cycleCountDto.BinLocation, cycleCountDto.Originator, cycleCountDto.RmPallets);
            }

            return wrapper;
        }

        [Route("emptypalletsinrack")]
        [HttpPost]
        public ActionResult<TransactionWrapper> EmptyPalletsInRack(CycleCountDto cycleCountDto)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            if (String.IsNullOrEmpty(cycleCountDto.Originator) || String.IsNullOrEmpty(cycleCountDto.BinLocation))
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("SavePalletsInRack : One or more string values empty");
                return wrapper;
            }
            else
            {
                wrapper = _cycleCountBusiness.EmptyPalletsInRack(cycleCountDto.BinLocation, cycleCountDto.Originator, cycleCountDto.IsEmpty);
            }

            return wrapper;
        }

        [Route("emptyrmpalletsinrack")]
        [HttpPost]
        public ActionResult<TransactionWrapper> EmptyRMPalletsInRack(CycleCountDto cycleCountDto)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            if (String.IsNullOrEmpty(cycleCountDto.Originator) || String.IsNullOrEmpty(cycleCountDto.BinLocation))
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("SavePalletsInRack : One or more string values empty");
                return wrapper;
            }
            else
            {
                wrapper = _cycleCountBusiness.EmptyRMPalletsInRack(cycleCountDto.BinLocation, cycleCountDto.Originator, cycleCountDto.IsEmpty);
            }

            return wrapper;
        }

    }
}