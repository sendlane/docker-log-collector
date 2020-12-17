from sendlane.google.sheets.api import GoogleSheet

gs = GoogleSheet()

sheet = gs.get_sheet()

print(sheet)

gs.append(
    [
        [0,1,2,3,4,5,6,7,8,9,0,1,2]
    ]
)
