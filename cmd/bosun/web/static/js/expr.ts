/// <reference path="0-bosun.ts" />
interface IExprScope extends IBosunScope {
	expr: string;
	error: string;
	running: string;
	result: any;
	queries: any;
	result_type: string;
	set: () => void;
	tab: string;
	graph: any;
	bar: any;
	svg_url: string;
	date: string;
	time: string;
	keydown: ($event: any) => void;
	animate: () => any;
	stop: () => any;
}

bosunControllers.controller('ExprCtrl', ['$scope', '$http', '$location', '$route', function($scope: IExprScope, $http: ng.IHttpService, $location: ng.ILocationService, $route: ng.route.IRouteService) {
	var search = $location.search();
	var current: string;
	try {
		current = atob(search.expr);
	}
	catch (e) {
		current = '';
	}
	if (!current) {
		$location.search('expr', btoa('avg(q("avg:rate:os.cpu{host=*bosun*}", "5m", "")) > 80'));
		return;
	}
	$scope.date = search.date || '';
	$scope.time = search.time || '';
	$scope.expr = current;

	$scope.running = current;
	$scope.tab = search.tab || 'results';
	$scope.animate();
	var dat = $scope.date ? $scope.date : ($scope.time ? moment().format("YYYY-MM-DD") : "");
	var ts = moment(dat + " " + $scope.time, "YYYY-MM-DD HH:mm:ss").utc();
	var not_empty = $scope.date || $scope.time;
	$http.post('/api/expr?' +
		'date=' + (not_empty ? encodeURIComponent(ts.format("YYYY-MM-DD")) : "") +
		'&time=' + (not_empty ? encodeURIComponent(ts.format("HH:mm:ss")) : ""), current)
		.success((data: any) => {
			$scope.result = data.Results;
			$scope.queries = data.Queries;
			$scope.result_type = data.Type;
			if (data.Type == 'series') {
				$scope.svg_url = '/api/egraph/' + btoa(current) + '.svg?now=' + Math.floor(Date.now() / 1000);
				$scope.graph = toChart(data.Results);
			}
			if (data.Type == 'number') {
				 angular.forEach(data.Results, (d) => {
					var name = '{';
					angular.forEach(d.Group, (tagv, tagk) => {
							if (name.length > 1) {
								 name += ',';
							}
							name += tagk + '=' + tagv;
					});
					name += '}';
					d.name = name;
				 });
				$scope.bar = data.Results;
			}
			$scope.running = '';
		})
		.error((error) => {
			$scope.error = error;
			$scope.running = '';
		})
		.finally(() => {
			$scope.stop();
		});
	$scope.set = () => {
		$location.search('expr', btoa($scope.expr));
		$location.search('date', $scope.date || null);
		$location.search('time', $scope.time || null);
		$route.reload();
	};
	function toChart(res: any) {
		var graph: any = [];
		angular.forEach(res, (d, idx) => {
			var data: any = [];
			angular.forEach(d.Value, (val, ts) => {
				data.push([+ts, val]);
			});
			if (data.length == 0) {
				return;
			}
			var name = '{';
			angular.forEach(d.Group, (tagv, tagk) => {
				if (name.length > 1) {
					name += ',';
				}
				name += tagk + '=' + tagv;
			});
			name += '}';
			var series = {
				Data: data,
				Name: name,
			};
			graph[idx] = series;
		});
		return graph;
	}
	$scope.keydown = function($event: any) {
		if ($event.shiftKey && $event.keyCode == 13) {
			$scope.set();
		}
	};

}]);
