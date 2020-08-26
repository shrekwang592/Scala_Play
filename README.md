/*
    In this coding challenge, you are being asked to create a subsystem for a finance organization.
    Given the following rules, create a scala program that will generate the required Accounting Totals (as defined below).

    Calculate the following Accounting Totals.
    1. Gross Sales (total of sales) Account#123456
       This is the sum of all sales for all sites. If a given site has no sales, post a '0' value.
    2. Inventory Value => Account#2983018
       This is the current value of the inventory in all sites. If a given site has no inventory, or is somehow negative, post that.
    3. Inventory Value Change From Yesterday => Account#3485719
       This is the difference between yesterday's and today's inventory value for a given site.
    4. Net Sales (Sales minus incoming inventory) => Account#4928392
       This is the amount of sales, but we subtract out new inventory value.

    Additional Technical Objectives:
    1. Be sure to have test cases created to properly test your code
    2. Create mock service implementations with mock data to test your code
    3. For coding style guide, use the core Scala Style Guide, for bonus points use the DataBricks Style Guide.

    Notes:
      For a given site, for a given date, only one accounting total can be made and posted to the General Ledger.
      Inventory Values are affected by both sales and inventory messages.
      If a sale or inventory movement is late, the value will be included in the next day's accounting total.
  */
