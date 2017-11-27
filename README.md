# Iguazio
<h3>Async home assignment from Iguazio</h3>
<ul>
    <li>Extract the zip to a folder</li>
    <li>For example: "C:\Iguazio"</li>
</ul>

<h4>Command line:</h4> 
```
	cd C:\Iguazio
	mvn clean install dependency:copy-dependencies
	java -cp "target\lib\*;target\home.assignment.jar" iguazio.home.assignment.BigBang "[input file]" "[output file]" "[endpoint1]" "[endpoint2]"
	For example:
	java -cp "target\lib\*;target\home.assignment.jar" iguazio.home.assignment.BigBang "c:\users\golaniz\desktop\input.txt" "c:\users\golaniz\desktop\output.txt" "http://localhost:1001" "http://localhost:1002"
```