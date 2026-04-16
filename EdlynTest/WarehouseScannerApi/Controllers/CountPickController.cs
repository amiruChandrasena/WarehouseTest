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
using Services.Ingres;

namespace WarehouseScannerApi.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class CountPickController : ControllerBase
    {
        private readonly CountPickBusiness _countPickBusiness;
        private readonly ICountPickService _countPickService;

        public CountPickController (ICountPickService countPickService)
        {
            _countPickService = countPickService;
            _countPickBusiness = new CountPickBusiness(_countPickService);
        }
        /*
        [Route("correct")]
        [HttpPost]
        public ActionResult<TransactionWrapper> Correct(CountPickDto countPickDto)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            if (countPickDto == null)
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("Correct : CountPickDto is null");
                return wrapper;
            }

            if (countPickDto.PalletLabels == null || countPickDto.PalletLabels.Count == 0)
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("Correct : No pallet labels attached to CountPickDto");
                return wrapper;
            }

            wrapper = _countPickBusiness.Correct(countPickDto);
            return wrapper;
        }*/

        [Route("getpicklocationdata/{binLocation}")]
        [HttpGet]
        public ActionResult<TransactionWrapper> GetPickLocationData(string binLocation)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            if (String.IsNullOrEmpty(binLocation))
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("GetPickLocationData : Bin location is empty");
                return wrapper;
            }
            else
            {
                wrapper = _countPickBusiness.GetPickLocationData(binLocation);
                return wrapper;
            }
        }

        [Route("save")]
        [HttpPost]
        public ActionResult<TransactionWrapper> Save(CountPickDto countPickDto)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            if (countPickDto == null)
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("Save : CountPickDto is null");
                return wrapper;
            }

            if (countPickDto.PalletLabels == null || countPickDto.PalletLabels.Count == 0)
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("Save : No pallet labels attached to CountPickDto");
                return wrapper;
            }

            wrapper = _countPickBusiness.Save(countPickDto);
            return wrapper;
        }
    }
}