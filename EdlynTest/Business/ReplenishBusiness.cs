using Abstractions.ServiceInterfaces;
using Models;
using System;
using System.Collections.Generic;
using System.Text;
using System.Transactions;

namespace Business
{
    public class ReplenishBusiness
    {
        private readonly IReplenishService _replenishService;

        public ReplenishBusiness(IReplenishService replenishService)
        {
            _replenishService = replenishService;
        }

        public TransactionWrapper GetBinLocations(string warehouseCode, string roomCode, bool isReplenish, bool isPullDown)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            if (isReplenish && isPullDown)
            {
                wrapper = _replenishService.GetBinLocationsToday(warehouseCode, roomCode);
            }
            else
            {
                wrapper = _replenishService.GetBinLocations(warehouseCode, roomCode);
            }

            return wrapper;
        }
        
        public TransactionWrapper GetBinLocationsHotList(string warehouseCode, string roomCode, bool isReplenish, bool isPullDown)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            if (isReplenish && isPullDown)
            {
                wrapper = _replenishService.GetBinLocationsToday(warehouseCode, roomCode);
            }
            else
            {
                wrapper = _replenishService.GetBinLocations(warehouseCode, roomCode);
            }

            return wrapper;
        }

        public TransactionWrapper GetNextSuggestedRack(string warehouseCode, string roomCode, string catalogCode, string bestBefore, int pickingSequence)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            wrapper = _replenishService.GetNextSuggestedRack(warehouseCode, roomCode, catalogCode, bestBefore, pickingSequence);
            return wrapper;
        }

        public TransactionWrapper GetPreviousSuggestedRack(string warehouseCode, string roomCode, string catalogCode, string bestBefore, int pickingSequence)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            wrapper = _replenishService.GetPreviousSuggestedRack(warehouseCode, roomCode, catalogCode, bestBefore, pickingSequence);
            return wrapper;
        }

        public TransactionWrapper GetReplenishItemDetails(string catalogCode, string warehouseCode, string roomCode)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            wrapper = _replenishService.GetReplenishItemDetails(catalogCode, warehouseCode, roomCode, false);

            return wrapper;
        }

        public TransactionWrapper GetSuggestedRack(string binLocation)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            try
            {
                int isPick = 0;
                string catalogCode = "";

                string[] locationParts = binLocation.Split('.');
                if (locationParts.Length != 3)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetSuggestedRack : Invalid bin location");
                    return wrapper;
                }

                string warehouseCode = locationParts[0];
                string roomCode = locationParts[1];

                wrapper = _replenishService.CheckIsPick(binLocation);

                if (wrapper.IsSuccess == false)
                {
                    return wrapper;
                }
                else
                {
                    isPick = (int)wrapper.ResultSet[0];
                    if (isPick == 1)
                    {
                        catalogCode = (string)wrapper.ResultSet[1];
                        wrapper.ResultSet.Clear();

                        wrapper = _replenishService.GetReplenishItemDetails(catalogCode, warehouseCode, roomCode, true);
                        if (wrapper.IsSuccess == false)
                        {
                            return wrapper;
                        }
                        ReplenishItem replenishItem = wrapper.ResultSet[0] as ReplenishItem;
                        wrapper.ResultSet.Clear();

                        // the following is awful and bad and should never be replicated but I'm on a time crunch, please forgive <3
                        PutAwayModel putAwayModel = new PutAwayModel();
                        PalletHeader palletHeader = new PalletHeader();
                        PalletDetail palletDetail = new PalletDetail();
                        CatalogItem catalogItem = new CatalogItem();
                        palletDetail.CatalogItem = catalogItem;
                        palletHeader.RoomType = replenishItem.RoomType;
                        palletHeader.PalletDetails.Add(palletDetail);
                        putAwayModel.Pallet = palletHeader;

                        putAwayModel.RoomType = replenishItem.RoomType;
                        putAwayModel.Description = "";
                        putAwayModel.PickingSequence = replenishItem.PickingSequence;
                        putAwayModel.IsPick = replenishItem.IsPick;
                        putAwayModel.CurrentBinLocation = binLocation;

                        putAwayModel.Pallet.PalletDetails[0].PalletUnits = replenishItem.PalletUnits;
                        if (!String.IsNullOrEmpty(replenishItem.CatalogCode))
                        {
                            putAwayModel.CatalogCode = replenishItem.CatalogCode;
                        }
                        else
                        {
                            putAwayModel.CatalogCode = "";
                        }
                        if (!String.IsNullOrEmpty(replenishItem.BinLocation))
                        {
                            putAwayModel.SuggestedRack = replenishItem.BinLocation;
                        }
                        if (replenishItem.BestBefore != null)
                        {
                            putAwayModel.Pallet.BestBefore = replenishItem.BestBefore;
                        }
                        else
                        {
                            putAwayModel.Pallet.PalletDetails[0].BestBefore = DateTime.MinValue;
                        }
                        wrapper.ResultSet.Add(putAwayModel);
                        wrapper.IsSuccess = true;
                        return wrapper;
                    }
                    else
                    {
                        PutAwayModel putAwayModel = new PutAwayModel();
                        putAwayModel.IsPick = false;
                        wrapper.ResultSet.Clear();
                        wrapper.ResultSet.Add(putAwayModel);
                        wrapper.IsSuccess = true;
                        return wrapper;
                    }
                }
            }
            catch (Exception e)
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add(e.Message);
                return wrapper;
            }
        }

        public TransactionWrapper ValidatePallet(int palletNo, bool isPullDown, bool isReplenish, string originator, string warehouseCode, string roomCode)
        {
            string logFileName = String.Format("ValidatePallet_{0}_{1}.txt", DateTime.Now.ToString("yyyyMMdd"), palletNo.ToString());

            TransactionWrapper wrapper = new TransactionWrapper();

            using (TransactionScope scope = new TransactionScope())
            {
                try
                {
                    PutAwayModel putAwayModel = new PutAwayModel(); // because Replenish uses a very similar layout to Put Away
                    PalletHeader pallet = new PalletHeader();

                    bool isMixedPallet = false;
                    //int isPick = 0;
                    string catalogCode = "";
                    //string warehouseCode = "";
                    //string roomCode = "";
                    string rackCode = "";

                    wrapper = _replenishService.GetPalletDetail(palletNo);
                    if (wrapper.IsSuccess == false)
                    {
                        return wrapper;
                    }

                    pallet = wrapper.ResultSet[0] as PalletHeader;
                    wrapper.ResultSet.Clear();

                    /*
                    wrapper = GetSuggestedRack(pallet.BinLocation);
                    if (wrapper.IsSuccess == false)
                    {
                        return wrapper;
                    }

                    if (wrapper.ResultSet.Count > 0)
                    {
                        ReplenishItem replenishItem = wrapper.ResultSet[0] as ReplenishItem;
                        putAwayModel.CatalogCode = replenishItem.CatalogCode;
                        putAwayModel.Pallet = pallet;
                        pallet.PalletDetails.Add(new PalletDetail());
                        putAwayModel.Pallet.BestBefore = replenishItem.BestBefore;
                        putAwayModel.PickingSequence = replenishItem.PickingSequence;
                        putAwayModel.CurrentBinLocation = pallet.BinLocation;
                        putAwayModel.SuggestedRack = replenishItem.BinLocation;
                        putAwayModel.Pallet.PalletDetails[0].PalletUnits = replenishItem.PalletUnits;
                        putAwayModel.IsPick = replenishItem.IsPick;

                        wrapper.ResultSet.Clear();
                        wrapper.ResultSet.Add(putAwayModel);
                        return wrapper;
                    }*/


                    string[] locationParts = pallet.BinLocation.Split('.');
                    if (locationParts.Length != 3)
                    {
                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add("Pallet has invalid bin location");
                        return wrapper;
                    }

                    //warehouseCode = locationParts[0];
                    //roomCode = locationParts[1];
                    rackCode = locationParts[2];

                    // the following is for PullDown which does not seem to be a feature for Edlyn (button is set to invisible in OpenROAD), code below added for completion sake
                    // as of now isPullDown will always be false

                    /* START
                    wrapper = _replenishService.CheckIsPick(pallet.BinLocation);
                    if (wrapper.IsSuccess == false)
                    {
                        return wrapper;
                    }

                    isPick = (int)wrapper.ResultSet[0];
                    catalogCode = (string)wrapper.ResultSet[1];
                    wrapper.ResultSet.Clear();

                    if (isPick == 1 && isPullDown == true)
                    {
                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add("Cannot pull down from pick phase");
                        return wrapper;
                    }
                    END */

                    // check if mixed pallet
                    if (pallet.PalletDetails.Count > 1)
                    {
                        catalogCode = pallet.PalletDetails[0].CatalogCode.Trim();
                        for (int i = 1; i < pallet.PalletDetails.Count; i++)
                        {
                            if (pallet.PalletDetails[i].CatalogCode.Trim() != catalogCode.Trim())
                            {
                                isMixedPallet = true;
                                putAwayModel.Description = "MIXED PALLET";
                                putAwayModel.CatalogCode = pallet.PalletDetails.Count.ToString() + " Products";
                                putAwayModel.CurrentBinLocation = pallet.BinLocation;
                                break;
                            }
                        }
                    }
                    else
                    {
                        catalogCode = pallet.PalletDetails[0].CatalogCode.Trim();
                    }

                    putAwayModel.Pallet = pallet;
                    string optionalRoomType = "";
                    string roomType = "";

                    if (isMixedPallet == false && !String.IsNullOrEmpty(catalogCode))
                    {
                        DateTime dateNow = DateTime.Now;
                        roomType = pallet.RoomType;

                        if (String.IsNullOrEmpty(pallet.RoomTypeStageTwo) || pallet.RoomTypeStageTwo == roomType) // check if single stage storage
                        {
                            if (dateNow < pallet.PrintDate.AddHours(pallet.PalletDetails[0].CatalogItem.MaxWaitingHours))
                            {
                                optionalRoomType = "%C%";
                            }
                        }
                        else if (!String.IsNullOrEmpty(pallet.RoomTypeStageTwo)) // if so, is stage two storage
                        {
                            if (dateNow < pallet.PrintDate.AddHours(pallet.PalletDetails[0].CatalogItem.MaxWaitingHours))
                            {
                                optionalRoomType = "%C%";
                            }
                            else
                            {
                                if (pallet.PalletDetails[0].CatalogItem.LabApprovalAfterStageOne == 1)
                                {
                                    if (pallet.KeepInStageRoom != 1)
                                    {
                                        roomType = pallet.RoomTypeStageTwo;
                                    }
                                }
                                else
                                {
                                    string currentRoomType = "";
                                    bool roomTypeAssigned = false;

                                    string message = _replenishService.GetRoomType(ref currentRoomType, warehouseCode, roomCode);

                                    if (!String.IsNullOrEmpty(currentRoomType))
                                    {
                                        char[] roomTypeStageTwoLetters = pallet.RoomTypeStageTwo.ToCharArray();
                                        if (currentRoomType.IndexOfAny(roomTypeStageTwoLetters) != -1)
                                        {
                                            Common.WriteLogFile.WriteLog(logFileName, String.Format("0.0.13. {0} - {1}", DateTime.Now.ToString(), "Validate Pallet " + palletNo.ToString() + " / "));

                                            roomType = pallet.RoomTypeStageTwo;
                                            roomTypeAssigned = true;
                                        }
                                    }

                                    if (roomTypeAssigned == false)
                                    {
                                        if (!String.IsNullOrEmpty(currentRoomType))
                                        {
                                            char[] roomTypeLetters = pallet.RoomType.ToCharArray();
                                            DateTime stageOneRackedTime = DateTime.MinValue;
                                            if (currentRoomType.IndexOfAny(roomTypeLetters) != -1)
                                            {
                                                try
                                                {
                                                    stageOneRackedTime = DateTime.Parse(pallet.StageOneRackedTime);
                                                }
                                                catch (Exception e)
                                                {
                                                    wrapper.IsSuccess = false;
                                                    wrapper.Messages.Add("StageOneRackedTime : " + e.Message);
                                                    return wrapper;
                                                }

                                                if (dateNow > pallet.RackedTime.AddHours(pallet.PalletDetails[0].CatalogItem.StageOneRoomHours) ||
                                                    dateNow > stageOneRackedTime.AddHours(pallet.PalletDetails[0].CatalogItem.StageOneRoomHours) && stageOneRackedTime != DateTime.MinValue)
                                                {
                                                    roomType = pallet.RoomTypeStageTwo;
                                                    roomTypeAssigned = true;
                                                    wrapper.IsSuccess = _replenishService.ReleaseFromStageOneRoom(pallet.PalletNumber);
                                                    if (wrapper.IsSuccess == false)
                                                    {
                                                        wrapper.Messages.Add("ReleaseFromStageOneRoom : Error");
                                                        return wrapper;
                                                    }

                                                    PalletLocationLog palletLocationLog = new PalletLocationLog
                                                    {
                                                        Timestamp = DateTime.Now,
                                                        PalletNo = pallet.PalletNumber,
                                                        NewLocation = "",
                                                        MovedBy = originator,
                                                        SyncTime = "",
                                                        Remark = "RELEASED FROM STAGE 1-A"
                                                    };

                                                    //Common.WriteLogFile.WriteLog(logFileName, String.Format("6. {0} - {1}", DateTime.Now.ToString(), "Validate Pallet " + palletNo.ToString() + " / "));

                                                    wrapper.IsSuccess = _replenishService.InsertPalletLocationLog(palletLocationLog);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        putAwayModel.Description = pallet.PalletDetails[0].CatalogItem.Description;
                        putAwayModel.CatalogCode = catalogCode;
                        putAwayModel.RoomType = roomType;
                        putAwayModel.CurrentBinLocation = pallet.BinLocation;
                        putAwayModel.OptionalRoomType = optionalRoomType;
                        putAwayModel.IsMixedPallet = isMixedPallet;
                        putAwayModel.IsPick = false;

                        if (isReplenish == true)
                        {
                            wrapper = _replenishService.GetAssignedLocationAndPickingSequence(warehouseCode, roomCode, pallet.PalletDetails[0].CatalogCode);
                            if (wrapper.IsSuccess == true)
                            {
                                putAwayModel.SuggestedRack = (string)wrapper.ResultSet[0];
                                putAwayModel.PickingSequence = (int)wrapper.ResultSet[1];
                            }
                            else
                            {
                                wrapper.Messages.Add("GetAssignedLocationAndPickingSequence : No record found for selected picking zone");
                                return wrapper;
                            }
                        }
                        // currently no case where isReplenish will not be true, if this changes implement else statement from OpenROAD fm_reach_forklift in ON SETVALUE pallet_no_ent
                    }

                    scope.Complete();
                    wrapper.ResultSet.Clear();
                    wrapper.ResultSet.Add(putAwayModel);
                    wrapper.IsSuccess = true;

                    return wrapper;
                }
                catch (Exception e)
                {
                    scope.Dispose();
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add(e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper ProcessRack(string warehouseCode, string roomCode, string rackCode, string originator, PutAwayModel bulkBin)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (TransactionScope scope = new TransactionScope())
            {
                try
                {
                    string pickBinLocation = warehouseCode + '.' + roomCode + '.' + rackCode;

                    // get pick location rack
                    wrapper = _replenishService.GetWarehouseRack(warehouseCode, roomCode, rackCode);
                    if (wrapper.IsSuccess == false)
                    {
                        scope.Dispose();
                        return wrapper;
                    }

                    WarehouseRack pickingRack = wrapper.ResultSet[0] as WarehouseRack;
                    wrapper.ResultSet.Clear();

                    // check if temporary hold rack
                    if (pickingRack.Status == "T")
                    {
                        scope.Dispose();
                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add(warehouseCode + "." + roomCode + "." + rackCode + " is a temporary hold rack");
                        return wrapper;
                    }

                    // if bulk location scanned, reject
                    if (pickingRack.IsPick == 0)
                    {
                        scope.Dispose();
                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add("Cannot replenish into BULK bin " + warehouseCode + "." + roomCode + "." + rackCode);
                        return wrapper;
                    }

                    // get warehouse config
                    wrapper = _replenishService.GetWarehouseConfig(warehouseCode, roomCode);
                    if (wrapper.IsSuccess == false)
                    {
                        scope.Dispose();
                        return wrapper;
                    }

                    WarehouseConfig whConfig = wrapper.ResultSet[0] as WarehouseConfig;
                    wrapper.ResultSet.Clear();

                    if (whConfig.SkipValidation == 0 && !bulkBin.IsMixedPallet)
                    {
                        // check if optional type allowed
                        if (!String.IsNullOrEmpty(bulkBin.OptionalRoomType))
                        {
                            if (whConfig.Type.Contains(bulkBin.OptionalRoomType))
                            {
                                whConfig.Type += bulkBin.RoomType;
                            }
                        }

                        // check if room and pallet types match
                        if (!whConfig.Type.Contains(bulkBin.RoomType))
                        {
                            scope.Dispose();
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add("Room type (" + whConfig.Type + ") and pallet type (" + bulkBin.RoomType + ") do not match");
                            return wrapper;
                        }
                    }

                    if (pickingRack.IsPick == 1)
                    {
                        if (!bulkBin.IsMixedPallet)
                        {
                            // check if picking and bulk locations have the same product
                            if (!String.IsNullOrEmpty(pickingRack.ReservedCatalogCode) && pickingRack.ReservedCatalogCode != bulkBin.CatalogCode)
                            {
                                scope.Dispose();
                                wrapper.IsSuccess = false;
                                wrapper.Messages.Add("Pick phase rack has been reserved for " + pickingRack.ReservedCatalogCode);
                                return wrapper;
                            }
                        }
                        else
                        {
                            int productCount = 0;
                            productCount = _replenishService.GetProductCountOnPallet(pickingRack.ReservedCatalogCode, bulkBin.Pallet.PalletNumber);
                            if (productCount == -1)
                            {
                                scope.Dispose();
                                wrapper.IsSuccess = false;
                                wrapper.Messages.Add("GetProductCountOnPallet: Error");
                                return wrapper;
                            }
                            else if (productCount == 0)
                            {
                                scope.Dispose();
                                wrapper.IsSuccess = false;
                                wrapper.Messages.Add("Mixed pallet " + bulkBin.Pallet.PalletNumber.ToString() + " does not have any of " + pickingRack.ReservedCatalogCode);
                                return wrapper;
                            }
                            else
                            {
                                bulkBin.SetPalletUnits = productCount;
                            }
                        }
                    }

                    // OpenROAD has "IF Li_isMixedPallet = FALSE AND :Lu_wh_room_config_ut.isPick = FALSE THEN" here, but we already check for isPick = false above and return if so, so this will never be
                    // true, hence it is skipped

                    string originalBinLocation = "";

                    originalBinLocation = bulkBin.Pallet.BinLocation;
                    string[] origRackParts = originalBinLocation.Split('.');

                    if (pickingRack.IsPick == 1) // this could be included in the above, kept separate to easier follow OpenROAD code
                    {
                        int licensedPalletNo = 0;
                        string reservedCatalogCode = "";

                        wrapper = _replenishService.GetRackLicensedPalletAndCatalogCode(ref licensedPalletNo, ref reservedCatalogCode, warehouseCode, roomCode, rackCode);
                        if (wrapper.IsSuccess == false)
                        {
                            scope.Dispose();
                            return wrapper;
                        }

                        if (licensedPalletNo == 0)
                        {
                            wrapper = _replenishService.UpdateRackWithLicensedPalletNo(bulkBin.Pallet.PalletNumber, bulkBin.CatalogCode, warehouseCode, roomCode, rackCode);
                            if (wrapper.IsSuccess == false)
                            {
                                scope.Dispose();
                                return wrapper;
                            }

                            wrapper = _replenishService.UpdatePalletPlanNo(licensedPalletNo);
                            if (wrapper.IsSuccess == false)
                            {
                                scope.Dispose();
                                return wrapper;
                            }
                        }
                        else if (reservedCatalogCode == bulkBin.CatalogCode)
                        {
                            wrapper = _replenishService.SetBinLocation(pickBinLocation, "W", licensedPalletNo);
                            if (wrapper.IsSuccess == false)
                            {
                                scope.Dispose();
                                return wrapper;
                            }

                            wrapper = _replenishService.UpdatePalletPlanNo(licensedPalletNo);
                            if (wrapper.IsSuccess == false)
                            {
                                scope.Dispose();
                                return wrapper;
                            }

                            wrapper = _replenishService.DeleteEmptyPalletDetail(licensedPalletNo);
                            if (wrapper.IsSuccess == false)
                            {
                                scope.Dispose();
                                return wrapper;
                            }

                            // update pick bin units
                            wrapper = _replenishService.UpdatePalletDetail(bulkBin.SetPalletUnits, licensedPalletNo, bulkBin.CatalogCode, bulkBin.Pallet.BestBefore);
                            if (wrapper.IsSuccess == false)
                            {
                                scope.Dispose();
                                return wrapper;
                            }

                            int rowCount = (int)wrapper.ResultSet[0];
                            if (rowCount == 0)
                            {
                                // insert pick bin units 
                                wrapper = _replenishService.InsertPalletDetail(licensedPalletNo, bulkBin.CatalogCode, bulkBin.SetPalletUnits, bulkBin.Pallet);
                                if (wrapper.IsSuccess == false)
                                {
                                    scope.Dispose();
                                    return wrapper;
                                }
                            }
                            else if (rowCount > 1)
                            {
                                scope.Dispose();
                                wrapper.IsSuccess = false;
                                wrapper.Messages.Add("Please count the rack first!");
                                return wrapper;
                            }
                            // update bulk bin, pallet units multiplied by -1 so the units are subtracted
                            wrapper = _replenishService.UpdatePalletDetail(bulkBin.SetPalletUnits * -1, bulkBin.Pallet.PalletNumber, bulkBin.CatalogCode, bulkBin.Pallet.BestBefore);
                            if (wrapper.IsSuccess == false)
                            {
                                scope.Dispose();
                                return wrapper;
                            }
                        }
                        else
                        {
                            scope.Dispose();
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add("Rack is reserved for " + reservedCatalogCode);
                            return wrapper;
                        }
                    }
                    // updating bulk bin stuff
                    int units = _replenishService.GetPalletUnits(bulkBin.Pallet.PalletNumber);
                    if (units == -1)
                    {
                        scope.Dispose();
                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add("Error getting pallet units");
                        return wrapper;
                    }

                    if (units <= 0)
                    {
                        wrapper = _replenishService.SetBinLocation(warehouseCode, roomCode, rackCode, "D", bulkBin.Pallet.PalletNumber);
                    }
                    else
                    {
                        wrapper = _replenishService.SetBinLocation(origRackParts[0], origRackParts[1], origRackParts[2], "W", bulkBin.Pallet.PalletNumber);
                    }

                    if (wrapper.IsSuccess == false)
                    {
                        scope.Dispose();
                        return wrapper;
                    }

                    string cellCountCatalogCode = "";
                    if (!bulkBin.IsMixedPallet)
                    {
                        cellCountCatalogCode = bulkBin.CatalogCode;
                    }

                    wrapper = _replenishService.SetUsedCellCount(pickBinLocation, warehouseCode, roomCode, rackCode, cellCountCatalogCode, bulkBin.Pallet.BestBefore);
                    if (wrapper.IsSuccess == false)
                    {
                        scope.Dispose();
                        return wrapper;
                    }

                    if (!String.IsNullOrEmpty(originalBinLocation))
                    {
                        int count = 0;
                        wrapper = _replenishService.GetRackCount(origRackParts[0], origRackParts[1], origRackParts[2]);
                        if (wrapper.IsSuccess == false)
                        {
                            scope.Dispose();
                            return wrapper;
                        }

                        count = (int)wrapper.ResultSet[0];
                        if (count == 1)
                        {
                            wrapper = _replenishService.SetUsedCellCount(originalBinLocation, origRackParts[0], origRackParts[1], origRackParts[2], "", DateTime.MinValue);
                            if (wrapper.IsSuccess == false)
                            {
                                scope.Dispose();
                                return wrapper;
                            }
                        }
                    }

                    PalletLocationLog palletLocationLog = new PalletLocationLog
                    {
                        Timestamp = DateTime.Now,
                        PalletNo = bulkBin.Pallet.PalletNumber,
                        NewLocation = warehouseCode + "." + roomCode + "." + rackCode,
                        MovedBy = originator,
                        SyncTime = "",
                        Remark = "HH REPLENISHED"
                    };

                    wrapper.IsSuccess = _replenishService.InsertPalletLocationLog(palletLocationLog);
                    if (wrapper.IsSuccess == false)
                    {
                        scope.Dispose();
                        return wrapper;
                    }

                    scope.Complete();

                    return wrapper;

                }
                catch (Exception e)
                {
                    scope.Dispose();
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add(e.Message);
                    return wrapper;
                }
            }
        }

        //public TransactionWrapper ProcessRack(string warehouseCode, string roomCode, string rackCode, string originator, PutAwayModel putAwayModel)
        //{
        //    TransactionWrapper errorWrapper = new TransactionWrapper(); // for debugging a null object exception without being able to access the database :-|
        //    try
        //    {
        //        TransactionWrapper wrapper = new TransactionWrapper();
        //        int setQuantity = putAwayModel.SetPalletUnits;

        //        wrapper = _replenishService.GetWarehouseRack(warehouseCode, roomCode, rackCode);
        //        if (wrapper.IsSuccess == false)
        //        {
        //            return wrapper;
        //        }

        //        WarehouseRack rack = wrapper.ResultSet[0] as WarehouseRack;
        //        wrapper.ResultSet.Clear();
        //        if (rack.Status == "T")
        //        {
        //            wrapper.IsSuccess = false;
        //            wrapper.Messages.Add(warehouseCode + "." + roomCode + "." + rackCode + " is a temporary hold rack.");
        //            return wrapper;
        //        }

        //        wrapper = _replenishService.GetWarehouseConfig(warehouseCode, roomCode);
        //        if (wrapper.IsSuccess == false)
        //        {
        //            return wrapper;
        //        }

        //        WarehouseConfig warehouseConfig = wrapper.ResultSet[0] as WarehouseConfig;
        //        wrapper.ResultSet.Clear();
        //        errorWrapper.Messages.Add("Accessing WarehouseConfig");
        //        if (warehouseConfig.SkipValidation == 0 || putAwayModel.IsMixedPallet == false)
        //        {
        //            if (!String.IsNullOrEmpty(putAwayModel.OptionalRoomType))
        //            {
        //                if (warehouseConfig.Type.Contains(putAwayModel.OptionalRoomType))
        //                {
        //                    warehouseConfig.Type += putAwayModel.RoomType;
        //                }
        //                else
        //                {
        //                    wrapper.IsSuccess = false;
        //                    wrapper.Messages.Add("Room type and pallet type do not match");
        //                    return wrapper;
        //                }
        //            }
        //        }

        //        errorWrapper.Messages.Add("Checked type indexes");
        //        if (rack.IsPick == 1)
        //        {
        //            if (putAwayModel.IsMixedPallet == false)
        //            {
        //                if (rack.ReservedCatalogCode != "" && rack.ReservedCatalogCode != putAwayModel.CatalogCode)
        //                {
        //                    if (rack.UnitsLeft <= 0)
        //                    {
        //                        wrapper.IsSuccess = false;
        //                        wrapper.Messages.Add("Pick phase rack " + rack.WarehouseCode + "." + rack.RoomCode + "." + rack.RackCode + " has been reserved for " + rack.ReservedCatalogCode);
        //                        return wrapper;
        //                    }
        //                }
        //            }
        //            else
        //            {
        //                int productCount = _replenishService.GetProductCountOnPallet(putAwayModel.Pallet.PalletNumber, putAwayModel.CatalogCode);
        //                if (productCount == -1)
        //                {
        //                    wrapper.IsSuccess = false;
        //                    wrapper.Messages.Add("GetProductCount : Exception thrown");
        //                    return wrapper;
        //                }

        //                if (productCount == 0)
        //                {
        //                    wrapper.IsSuccess = false;
        //                    wrapper.Messages.Add("Mixed pallet " + putAwayModel.Pallet.PalletNumber + " does not have any units of " + rack.ReservedCatalogCode + " left.");
        //                    return wrapper;
        //                }
        //            }
        //        }

        //        if (putAwayModel.IsMixedPallet == false && rack.IsPick == 0)
        //        {
        //            if (rack.ReservedCatalogCode != "" && rack.ReservedCatalogCode != putAwayModel.CatalogCode)
        //            {
        //                wrapper.IsSuccess = false;
        //                wrapper.Messages.Add("Rack has been reserved for " + putAwayModel.CatalogCode);
        //                return wrapper;
        //            }

        //            if (warehouseConfig.SkipValidation == 0)
        //            {
        //                wrapper = _replenishService.CheckProductsInRack(warehouseCode, roomCode, rackCode, putAwayModel.CatalogCode);
        //                if (wrapper.IsSuccess == false)
        //                {
        //                    return wrapper;
        //                }
        //            }
        //        }

        //        string originalBinLocation = putAwayModel.CurrentBinLocation;
        //        using (TransactionScope scope = new TransactionScope())
        //        {
        //            if (rack.IsPick == 1)
        //            {
        //                DateTime dateNow = DateTime.Now;

        //                int licensedPalletNumber = 0;
        //                string reservedCatalogCode = "";

        //                wrapper = _replenishService.GetRackLicensedPalletAndCatalogCode(ref licensedPalletNumber, ref reservedCatalogCode, warehouseCode, roomCode, rackCode);
        //                if (wrapper.IsSuccess == false)
        //                {
        //                    scope.Dispose();
        //                    return wrapper;
        //                }

        //                if (licensedPalletNumber == 0)
        //                {
        //                    wrapper = _replenishService.SetLicensedPallet(putAwayModel.Pallet.PalletNumber, putAwayModel.CatalogCode, warehouseCode, roomCode, rackCode);
        //                    if (wrapper.IsSuccess == false)
        //                    {
        //                        scope.Dispose();
        //                        return wrapper;
        //                    }


        //                    // licensed pallet number will always be zero here, left in because couldn't get a straight answer on why it's here.
        //                    wrapper = _replenishService.SetPlanNumber(licensedPalletNumber);
        //                    if (wrapper.IsSuccess == false)
        //                    {
        //                        scope.Dispose();
        //                        return wrapper;
        //                    }
        //                }
        //                else if (reservedCatalogCode == putAwayModel.CatalogCode)
        //                {
        //                    string binLocation = warehouseCode + "." + roomCode + "." + rackCode;
        //                    wrapper = _replenishService.SetBinLocation(binLocation, licensedPalletNumber);
        //                    if (wrapper.IsSuccess == false)
        //                    {
        //                        scope.Dispose();
        //                        return wrapper;
        //                    }

        //                    wrapper = _replenishService.SetPlanNumber(licensedPalletNumber);
        //                    if (wrapper.IsSuccess == false)
        //                    {
        //                        scope.Dispose();
        //                        return wrapper;
        //                    }

        //                    wrapper = _replenishService.DeleteEmptyPalletDetail(licensedPalletNumber);
        //                    if (wrapper.IsSuccess == false)
        //                    {
        //                        scope.Dispose();
        //                        return wrapper;
        //                    }

        //                    wrapper = _replenishService.GetPalletDetailForUpdate(putAwayModel.Pallet.PalletNumber, putAwayModel.CatalogCode);
        //                    if (wrapper.IsSuccess == false)
        //                    {
        //                        scope.Dispose();
        //                        return wrapper;
        //                    }

        //                    if (wrapper.ResultSet.Count > 0)
        //                    {
        //                        errorWrapper.Messages.Add("Looping through Pallet Detail");
        //                        foreach (object obj in wrapper.ResultSet)
        //                        {
        //                            PalletDetail palletDetail = obj as PalletDetail;
        //                            if (setQuantity <= 0)
        //                            {
        //                                break;
        //                            }
        //                            else if (palletDetail.PalletUnits >= setQuantity)
        //                            {
        //                                palletDetail.PalletUnits = setQuantity;
        //                                setQuantity = 0;
        //                            }
        //                            else if (palletDetail.PalletUnits < setQuantity)
        //                            {
        //                                setQuantity -= palletDetail.PalletUnits;
        //                            }

        //                            TransactionWrapper updateWrapper = _replenishService.UpdatePalletDetail(palletDetail.PalletUnits, licensedPalletNumber, putAwayModel.CatalogCode, palletDetail.BestBefore);
        //                            if (updateWrapper.IsSuccess == false)
        //                            {
        //                                scope.Dispose();
        //                                return updateWrapper;
        //                            }
        //                            errorWrapper.Messages.Add("Getting rows affected converted to int");
        //                            int rowsAffected = (int)updateWrapper.ResultSet[0];
        //                            updateWrapper.ResultSet.Clear();

        //                            if (rowsAffected == 0)
        //                            {
        //                                updateWrapper = _replenishService.InsertPalletDetail(licensedPalletNumber, putAwayModel.Pallet.PalletNumber, putAwayModel.CatalogCode, palletDetail);
        //                                if (updateWrapper.IsSuccess == false)
        //                                {
        //                                    scope.Dispose();
        //                                    return updateWrapper;
        //                                }
        //                            }
        //                            else if (rowsAffected > 1)
        //                            {
        //                                scope.Dispose();
        //                                updateWrapper.IsSuccess = false;
        //                                updateWrapper.Messages.Add("Please count the rack first");
        //                                return updateWrapper;
        //                            }

        //                            updateWrapper = _replenishService.UpdateOldPalletDetail(palletDetail, putAwayModel.Pallet.PalletNumber, putAwayModel.CatalogCode);
        //                            if (updateWrapper.IsSuccess == false)
        //                            {
        //                                scope.Dispose();
        //                                return updateWrapper;
        //                            }
        //                        }
        //                    }
        //                }
        //                else
        //                {
        //                    scope.Dispose();
        //                    wrapper.IsSuccess = false;
        //                    wrapper.Messages.Add("Please count rack first");
        //                    return wrapper;
        //                }
        //            }
        //            int units = _replenishService.GetPalletUnits(putAwayModel.Pallet.PalletNumber);
        //            if (units == -1)
        //            {
        //                scope.Dispose();
        //                wrapper.IsSuccess = false;
        //                wrapper.Messages.Add("GetPalletUnits : Error");
        //                return wrapper;
        //            }

        //            string status = "";
        //            if (units == 0)
        //            {
        //                status = "D";
        //            }
        //            else
        //            {
        //                status = "W";
        //            }
        //            wrapper = _replenishService.SetBinLocation(warehouseCode, roomCode, rackCode, status, putAwayModel.Pallet.PalletNumber);

        //            string rackCatalogCode = "";
        //            if (!putAwayModel.IsMixedPallet)
        //            {
        //                rackCatalogCode = putAwayModel.CatalogCode;
        //            }

        //            wrapper = _replenishService.SetUsedCellCount(warehouseCode, roomCode, rackCode, rackCatalogCode, putAwayModel.Pallet.BestBefore);

        //            if (wrapper.IsSuccess == false)
        //            {
        //                scope.Dispose();
        //                return wrapper;
        //            }

        //            if (!String.IsNullOrEmpty(originalBinLocation))
        //            {
        //                string[] originalParts = originalBinLocation.Split('.');
        //                wrapper = _replenishService.GetRackCount(originalParts[0], originalParts[1], originalParts[2]);
        //                if (wrapper.IsSuccess == false)
        //                {
        //                    scope.Dispose();
        //                    return wrapper;
        //                }
        //                errorWrapper.Messages.Add("Getting rack count as int");
        //                if ((int)wrapper.ResultSet[0] == 1)
        //                {
        //                    wrapper = _replenishService.SetUsedCellCount(originalParts[0], originalParts[1], originalParts[2], "", DateTime.MinValue);
        //                    if (wrapper.IsSuccess == false)
        //                    {
        //                        scope.Dispose();
        //                        return wrapper;
        //                    }
        //                }
        //            }

        //            scope.Complete();
        //        }

        //        PalletLocationLog palletLocationLog = new PalletLocationLog
        //        {
        //            Timestamp = DateTime.Now,
        //            PalletNo = putAwayModel.Pallet.PalletNumber,
        //            NewLocation = warehouseCode + "." + roomCode + "." + rackCode,
        //            MovedBy = originator,
        //            SyncTime = "",
        //            Remark = "HH REPLENISHED"
        //        };

        //        wrapper.IsSuccess = _replenishService.InsertPalletLocationLog(palletLocationLog);

        //        wrapper.IsSuccess = true;
        //        return wrapper;
        //    } 
        //    catch (Exception e)
        //    {
        //        errorWrapper.IsSuccess = false;
        //        errorWrapper.Messages.Add("ProcessRack : " + e.Message);
        //        return errorWrapper;
        //    }
        //}
    }
}

