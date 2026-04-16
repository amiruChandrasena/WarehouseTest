using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Abstractions.ServiceInterfaces;
using Models;

namespace WarehouseScannerApi.Controllers
{
    [Route("api/[controller]")]
    public class LoginController : ControllerBase
    {
        private readonly ILoginService _loginService;

        public LoginController(ILoginService loginService)
        {
            _loginService = loginService;
        }

        /// <summary>
        /// Get All Warehouses for user
        /// </summary>
        /// <returns></returns>
        [Route("getwarehouses")]
        [HttpGet]
        public ActionResult<TransactionWrapper> GetWarehouseIdName()
        {
            TransactionWrapper wrapper = _loginService.GetWarehouseIDName();
            return wrapper;
        }

        /// <summary>
        /// Get all racking zones for user
        /// </summary>
        /// <returns></returns>
        [Route("getrackingzones")]
        [HttpGet]
        public ActionResult<TransactionWrapper> GetRackingZones()
        {
            TransactionWrapper wrapper = _loginService.GetRackingZones();
            return wrapper;
        }

        /// <summary>
        /// Confirm user is valid and allow login
        /// </summary>
        /// <param name="fOperator"></param>
        /// <returns></returns>
        [Route("submit")]
        [HttpPost]
        public ActionResult<TransactionWrapper> GetForkliftOperator([FromBody]ForkliftOperator fOperator)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            if (fOperator == null)
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("Null object");
                return wrapper;
            }
            wrapper = _loginService.GetForkliftOperator(fOperator);
            return wrapper;
        }

        [Route("getuserdefaults/{userId}")]
        [HttpGet]
        public ActionResult<TransactionWrapper> GetUserDefaults(string userid)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            if (String.IsNullOrEmpty(userid))
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("Empty UserId");
                return wrapper;
            }
            else
            {
                wrapper = _loginService.GetUserDefaults(userid);
                return wrapper;
            }
        }
        

    }
}